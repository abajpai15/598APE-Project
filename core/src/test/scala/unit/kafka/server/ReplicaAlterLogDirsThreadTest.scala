/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.cluster.Partition
import kafka.log.LogManager
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.QuotaFactory.UNBOUNDED_QUOTA
import kafka.server.ReplicaAlterLogDirsThread.ReassignmentState
import kafka.server.metadata.KRaftMetadataCache
import kafka.utils.TestUtils
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.common
import org.apache.kafka.server.common.{DirectoryEventHandler, KRaftVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito.{doNothing, mock, never, times, verify, verifyNoInteractions, verifyNoMoreInteractions, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ReplicaAlterLogDirsThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val topicId = Uuid.randomUuid()
  private val topicNames = collection.immutable.Map(topicId -> "topic1")
  private val tid1p0 = new TopicIdPartition(topicId, t1p0)
  private val failedPartitions = new FailedPartitions
  private val metadataCache = new KRaftMetadataCache(1, () => KRaftVersion.LATEST_PRODUCTION)

  private def initialFetchState(fetchOffset: Long, leaderEpoch: Int = 1): InitialFetchState = {
    InitialFetchState(topicId = Some(topicId), leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @Test
  def shouldNotAddPartitionIfFutureLogIsNotDefined(): Unit = {
    val brokerId = 1
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])

    when(replicaManager.futureLogExists(t1p0)).thenReturn(false)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    val addedPartitions = thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))
    assertEquals(Set.empty, addedPartitions)
    assertEquals(0, thread.partitionCount)
    assertEquals(None, thread.fetchState(t1p0))
  }

  @Test
  def shouldUpdateLeaderEpochAfterFencedEpochError(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("gOZOXHnkR9eiA1W9ZuLk8A")))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val fencedRequestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch - 1))
    val fencedResponseData = new FetchPartitionData(
      Errors.FENCED_LEADER_EPOCH,
      -1,
      -1,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, fencedRequestData, config, replicaManager, fencedResponseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-log-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    // Initially we add the partition with an older epoch which results in an error
    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch - 1)))
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    thread.doWork()

    assertTrue(failedPartitions.contains(t1p0))
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)

    // Next we update the epoch and assert that we can continue
    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))
    assertEquals(Some(leaderEpoch), thread.fetchState(t1p0).map(_.currentLeaderEpoch))
    assertEquals(1, thread.partitionCount)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    thread.doWork()

    assertFalse(failedPartitions.contains(t1p0))
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  @Test
  def shouldReplaceCurrentLogDirWhenCaughtUp(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("PGLOjDjKQaCOXFOtxymIig")))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    thread.doWork()

    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
  }

  private def updateReassignmentState(thread: ReplicaAlterLogDirsThread, partitionId:Int, newState: ReassignmentState) = {
    topicNames.get(topicId).map(topicName => {
      thread.updateReassignmentState(new TopicPartition(topicName, partitionId), newState)
    })
  }

  @Test
  def shouldReplaceCurrentLogDirWhenCaughtUpWithAfterAssignmentRequestHasBeenCompleted(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])

    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0
    val currentDirectoryId = Uuid.fromString("EzI9SqkFQKW1iFc1ZwP9SQ")

    when(partition.partitionId).thenReturn(partitionId)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.futureReplicaDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.runCallbackIfFutureReplicaCaughtUp(any())).thenReturn(true)
    when(partition.logDirectoryId()).thenReturn(Some(currentDirectoryId))

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs,
      directoryEventHandler)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))

    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    // Don't promote future replica if no assignment state for this partition
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    updateReassignmentState(thread, partitionId, ReassignmentState.Queued)

    // Don't promote future replica if assignment request is queued but not completed
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)
    updateReassignmentState(thread, partitionId, ReassignmentState.Accepted)

    // Promote future replica if assignment request is completed
    thread.doWork()
    assertEquals(None, thread.fetchState(t1p0))
    assertEquals(0, thread.partitionCount)
    verifyNoInteractions(directoryEventHandler)

  }

  @Test
  def shouldRevertAnyScheduledAssignmentRequestIfAssignmentIsCancelled(): Unit = {
    val brokerId = 1
    val partitionId = 0
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId))

    val partition = Mockito.mock(classOf[Partition])
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])

    val futureLog = Mockito.mock(classOf[UnifiedLog])

    val leaderEpoch = 5
    val logEndOffset = 0

    when(partition.partitionId).thenReturn(partitionId)
    when(partition.topicId).thenReturn(Some(topicId))
    when(partition.futureReplicaDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.randomUuid()))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)

    when(quotaManager.isQuotaExceeded).thenReturn(false)

    when(partition.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(logEndOffset))
    when(partition.futureLocalLogOrException).thenReturn(futureLog)
    doNothing().when(partition).truncateTo(offset = 0, isFuture = true)
    when(partition.maybeReplaceCurrentWithFutureReplica()).thenReturn(true)
    when(partition.runCallbackIfFutureReplicaCaughtUp(any())).thenReturn(true)

    when(futureLog.logStartOffset).thenReturn(0L)
    when(futureLog.logEndOffset).thenReturn(0L)
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    val requestData = new FetchRequest.PartitionData(topicId, 0L, 0L,
      config.replicaFetchMaxBytes, Optional.of(leaderEpoch))
    val responseData = new FetchPartitionData(
      Errors.NONE,
      0L,
      0L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false)
    mockFetchFromCurrentLog(tid1p0, requestData, config, replicaManager, responseData)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      new BrokerTopicStats,
      config.replicaFetchBackoffMs,
      directoryEventHandler)

    thread.addPartitions(Map(t1p0 -> initialFetchState(fetchOffset = 0L, leaderEpoch)))

    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    // Don't promote future replica if no assignment state for this partition
    thread.doWork()
    assertTrue(thread.fetchState(t1p0).isDefined)
    assertEquals(1, thread.partitionCount)

    updateReassignmentState(thread, partitionId, ReassignmentState.Queued)

    // revert assignment and delete request state if assignment is cancelled
    thread.removePartitions(Set(t1p0))
    assertTrue(thread.fetchState(t1p0).isEmpty)
    assertEquals(0, thread.partitionCount)
    val topicIdPartitionCaptureT1p0: ArgumentCaptor[org.apache.kafka.server.common.TopicIdPartition] =
      ArgumentCaptor.forClass(classOf[org.apache.kafka.server.common.TopicIdPartition])
    val logIdCaptureT1p0: ArgumentCaptor[Uuid] = ArgumentCaptor.forClass(classOf[Uuid])

    verify(directoryEventHandler).handleAssignment(topicIdPartitionCaptureT1p0.capture(), logIdCaptureT1p0.capture(),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())

    assertEquals(new org.apache.kafka.server.common.TopicIdPartition(topicId, t1p0.partition()), topicIdPartitionCaptureT1p0.getValue)
    assertEquals(partition.logDirectoryId().get, logIdCaptureT1p0.getValue)
  }

  @Test
  def shouldRevertReassignmentsForIncompleteFutureReplicaPromotions(): Unit = {
    val replicaManager = Mockito.mock(classOf[ReplicaManager])
    val directoryEventHandler = mock(classOf[DirectoryEventHandler])
    val quotaManager = Mockito.mock(classOf[ReplicationQuotaManager])
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      Mockito.mock(classOf[BrokerTopicStats]),
      0,
      directoryEventHandler)

    val tp = Seq.range(0, 4).map(new TopicPartition("t", _))
    val tips = Seq.range(0, 4).map(new common.TopicIdPartition(topicId, _))
    val dirIds = Seq.range(0, 4).map(i => Uuid.fromString(s"TESTBROKER0000DIR${i}AAAA"))
    tp.foreach(tp => thread.promotionStates.put(tp, ReplicaAlterLogDirsThread.PromotionState(ReassignmentState.None, Some(topicId), Some(dirIds(tp.partition())))))
    thread.updateReassignmentState(tp(0), ReassignmentState.None)
    thread.updateReassignmentState(tp(1), ReassignmentState.Queued)
    thread.updateReassignmentState(tp(2), ReassignmentState.Accepted)
    thread.updateReassignmentState(tp(3), ReassignmentState.Effective)

    thread.removePartitions(tp.toSet)

    verify(directoryEventHandler).handleAssignment(ArgumentMatchers.eq(tips(1)), ArgumentMatchers.eq(dirIds(1)),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())
    verify(directoryEventHandler).handleAssignment(ArgumentMatchers.eq(tips(2)), ArgumentMatchers.eq(dirIds(2)),
      ArgumentMatchers.eq("Reverting reassignment for canceled future replica"), any())
    verifyNoMoreInteractions(directoryEventHandler)
  }

  private def mockFetchFromCurrentLog(topicIdPartition: TopicIdPartition,
                                      requestData: FetchRequest.PartitionData,
                                      config: KafkaConfig,
                                      replicaManager: ReplicaManager,
                                      responseData: FetchPartitionData): Unit = {
    val callbackCaptor: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] =
      ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val expectedFetchParams = new FetchParams(
      FetchRequest.FUTURE_LOCAL_REPLICA_ID,
      -1,
      0L,
      0,
      config.replicaFetchResponseMaxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )

    when(replicaManager.fetchMessages(
      params = ArgumentMatchers.eq(expectedFetchParams),
      fetchInfos = ArgumentMatchers.eq(Seq(topicIdPartition -> requestData)),
      quota = ArgumentMatchers.eq(UNBOUNDED_QUOTA),
      responseCallback = callbackCaptor.capture(),
    )).thenAnswer(_ => {
      callbackCaptor.getValue.apply(Seq((topicIdPartition, responseData)))
    })
  }

  @Test
  def issuesEpochRequestFromLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))

    //Setup all dependencies

    val partitionT1p0: Partition = mock(classOf[Partition])
    val partitionT1p1: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val partitionT1p0Id = 0
    val partitionT1p1Id = 1
    val leaderEpochT1p0 = 2
    val leaderEpochT1p1 = 5
    val leoT1p0 = 13
    val leoT1p1 = 232

    //Stubs
    when(partitionT1p0.partitionId).thenReturn(partitionT1p0Id)
    when(partitionT1p0.partitionId).thenReturn(partitionT1p1Id)

    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partitionT1p0)
    when(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p0, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p0Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p0)
        .setEndOffset(leoT1p0))

    when(replicaManager.getPartitionOrException(t1p1))
      .thenReturn(partitionT1p1)
    when(partitionT1p1.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpochT1p1, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p1Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p1)
        .setEndOffset(leoT1p1))

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, null)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      null,
      null,
      config.replicaFetchBackoffMs)

    val result = thread.leader.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpochT1p0),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpochT1p1)))

    val expected = Map(
      t1p0 -> new EpochEndOffset()
        .setPartition(t1p0.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p0)
        .setEndOffset(leoT1p0),
      t1p1 -> new EpochEndOffset()
        .setPartition(t1p1.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpochT1p1)
        .setEndOffset(leoT1p1)
    )

    assertEquals(expected, result, "results from leader epoch request should have offset from local replica")
  }

  @Test
  def fetchEpochsFromLeaderShouldHandleExceptionFromGetLocalReplica(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))

    //Setup all dependencies
    val partitionT1p0: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val partitionId = 0
    val leaderEpoch = 2
    val leo = 13

    //Stubs
    when(partitionT1p0.partitionId).thenReturn(partitionId)

    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partitionT1p0)
    when(partitionT1p0.lastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(leo))

    when(replicaManager.getPartitionOrException(t1p1))
      .thenThrow(new KafkaStorageException)

    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, null)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      null,
      null,
      config.replicaFetchBackoffMs)

    val result = thread.leader.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(leaderEpoch),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(leaderEpoch)))

    val expected = Map(
      t1p0 -> new EpochEndOffset()
        .setPartition(t1p0.partition)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(leo),
      t1p1 -> new EpochEndOffset()
        .setPartition(t1p1.partition)
        .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
    )

    assertEquals(expected, result)
  }

  @Test
  def shouldTruncateToReplicaOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateCaptureT1p0: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])
    val truncateCaptureT1p1: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val logT1p0: UnifiedLog = mock(classOf[UnifiedLog])
    val logT1p1: UnifiedLog = mock(classOf[UnifiedLog])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureLogT1p0: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLogT1p1: UnifiedLog = mock(classOf[UnifiedLog])
    val partitionT1p0: Partition = mock(classOf[Partition])
    val partitionT1p1: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val partitionT1p0Id = 0
    val partitionT1p1Id = 1
    val leaderEpoch = 2
    val futureReplicaLEO = 191
    val replicaT1p0LEO = 190
    val replicaT1p1LEO = 192

    //Stubs
    when(partitionT1p0.partitionId).thenReturn(partitionT1p0Id)
    when(partitionT1p1.partitionId).thenReturn(partitionT1p1Id)

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partitionT1p0)
    when(replicaManager.getPartitionOrException(t1p1))
      .thenReturn(partitionT1p1)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLogT1p0)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.futureLocalLogOrException(t1p1)).thenReturn(futureLogT1p1)
    when(replicaManager.futureLogExists(t1p1)).thenReturn(true)

    when(futureLogT1p0.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLogT1p1.logEndOffset).thenReturn(futureReplicaLEO)

    when(futureLogT1p0.latestEpoch).thenReturn(Optional.of(leaderEpoch))
    when(futureLogT1p0.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))
    when(partitionT1p0.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p0Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(replicaT1p0LEO))

    when(futureLogT1p1.latestEpoch).thenReturn(Optional.of(leaderEpoch))
    when(futureLogT1p1.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))
    when(partitionT1p1.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionT1p1Id)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(replicaT1p1LEO))
    when(partitionT1p0.logDirectoryId()).thenReturn(Some(Uuid.fromString("Jsg8ufNCQYONNquPt7VYpA")))
    when(partitionT1p1.logDirectoryId()).thenReturn(Some(Uuid.fromString("D2Yf6FtNROGVKoIZadSFIg")))

    when(replicaManager.logManager).thenReturn(logManager)
    stubWithFetchMessages(logT1p0, logT1p1, futureLogT1p0, partitionT1p0, replicaManager, responseCallback)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L), t1p1 -> initialFetchState(0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    verify(partitionT1p0).truncateTo(truncateCaptureT1p0.capture(), anyBoolean())
    verify(partitionT1p1).truncateTo(truncateCaptureT1p1.capture(), anyBoolean())
    assertEquals(replicaT1p0LEO, truncateCaptureT1p0.getValue)
    assertEquals(futureReplicaLEO, truncateCaptureT1p1.getValue)
  }

  @Test
  def shouldTruncateToEndOffsetOfLargestCommonEpoch(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    // one future replica mock because our mocking methods return same values for both future replicas
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val partitionId = 0
    val leaderEpoch = 5
    val futureReplicaLEO = 195
    val replicaLEO = 200
    val replicaEpochEndOffset = 190
    val futureReplicaEpochEndOffset = 191

    //Stubs
    when(partition.partitionId).thenReturn(partitionId)

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partition)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)

    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLog.latestEpoch)
      .thenReturn(Optional.of(leaderEpoch))
      .thenReturn(Optional.of(leaderEpoch - 2))

    // leader replica truncated and fetched new offsets with new leader epoch
    when(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch - 1)
        .setEndOffset(replicaLEO))
    // but future replica does not know about this leader epoch, so returns a smaller leader epoch
    when(futureLog.endOffsetForEpoch(leaderEpoch - 1)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch - 2)))
    // finally, the leader replica knows about the leader epoch and returns end offset
    when(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch - 2, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(leaderEpoch - 2)
        .setEndOffset(replicaEpochEndOffset))
    when(futureLog.endOffsetForEpoch(leaderEpoch - 2)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaEpochEndOffset, leaderEpoch - 2)))
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("n6WOe2zPScqZLIreCWN6Ug")))

    when(replicaManager.logManager).thenReturn(logManager)
    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // First run will result in another offset for leader epoch request
    thread.doWork()
    // Second run should actually truncate
    thread.doWork()

    //We should have truncated to the offsets in the response
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), ArgumentMatchers.eq(true))
    assertTrue(truncateToCapture.getAllValues.asScala.contains(replicaEpochEndOffset),
               "Expected offset " + replicaEpochEndOffset + " in captured truncation offsets " + truncateToCapture.getAllValues)
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfReplicaReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val initialFetchOffset = 100

    //Stubs
    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partition)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)

    when(replicaManager.logManager).thenReturn(logManager)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("b2e1ihvGQiu6A504oKoddQ")))

    // pretend this is a completely new future replica, with no leader epochs recorded
    when(futureLog.latestEpoch).thenReturn(Optional.empty)

    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(initialFetchOffset)))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    verify(partition).truncateTo(truncated.capture(), isFuture = ArgumentMatchers.eq(true))
    assertEquals(initialFetchOffset,
                 truncated.getValue, "Expected future replica to truncate to initial fetch offset if replica returns UNDEFINED_EPOCH_OFFSET")
  }

  @Test
  def shouldPollIndefinitelyIfReplicaNotAvailable(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val partitionId = 0
    val futureReplicaLeaderEpoch = 1
    val futureReplicaLEO = 290
    val replicaLEO = 300

    //Stubs
    when(partition.partitionId).thenReturn(partitionId)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("wO7bUpvcSZC0QKEK6P6AiA")))

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0))
      .thenReturn(partition)

    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLog.latestEpoch).thenReturn(Optional.of(futureReplicaLeaderEpoch))
    when(futureLog.endOffsetForEpoch(futureReplicaLeaderEpoch)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, futureReplicaLeaderEpoch)))
    when(replicaManager.localLog(t1p0)).thenReturn(Some(log))

    // this will cause fetchEpochsFromLeader return an error with undefined offset
    when(partition.lastOffsetForLeaderEpoch(Optional.of(1), futureReplicaLeaderEpoch, fetchOnlyFromLeader = false))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code))
      .thenReturn(new EpochEndOffset()
        .setPartition(partitionId)
        .setErrorCode(Errors.NONE.code)
        .setLeaderEpoch(futureReplicaLeaderEpoch)
        .setEndOffset(replicaLEO))

    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      responseCallback.capture(),
    )).thenAnswer(_ => responseCallback.getValue.apply(Seq.empty[(TopicIdPartition, FetchPartitionData)]))

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // Run thread 3 times (exactly number of times we mock exception for getReplicaOrException)
    (0 to 2).foreach { _ =>
      thread.doWork()
    }

    // Nothing happened since the replica was not available
    verify(partition, never()).truncateTo(truncated.capture(), isFuture = ArgumentMatchers.eq(true))
    assertEquals(0, truncated.getAllValues.size())

    // Next time we loop, getReplicaOrException will return replica
    thread.doWork()

    // Now the final call should have actually done a truncation (to offset futureReplicaLEO)
    verify(partition).truncateTo(truncated.capture(), isFuture = ArgumentMatchers.eq(true))
    assertEquals(futureReplicaLEO, truncated.getValue)
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnly(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit] = ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit])

    val partitionId = 0
    val leaderEpoch = 5
    val futureReplicaLEO = 190
    val replicaLEO = 213

    when(partition.partitionId).thenReturn(partitionId)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("dybMM9CpRP2s6HSslW4NHg")))

    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0))
        .thenReturn(partition)
    when(partition.lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false))
        .thenReturn(new EpochEndOffset()
          .setPartition(partitionId)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(leaderEpoch)
          .setEndOffset(replicaLEO))

    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(futureLog.latestEpoch).thenReturn(Optional.of(leaderEpoch))
    when(futureLog.logEndOffset).thenReturn(futureReplicaLEO)
    when(futureLog.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Optional.of(new OffsetAndEpoch(futureReplicaLEO, leaderEpoch)))
    when(replicaManager.logManager).thenReturn(logManager)
    stubWithFetchMessages(log, null, futureLog, partition, replicaManager, responseCallback)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(t1p0 -> initialFetchState(0L)))

    // loop few times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Assert that truncate to is called exactly once (despite more loops)
    verify(partition).lastOffsetForLeaderEpoch(Optional.of(1), leaderEpoch, fetchOnlyFromLeader = false)
    verify(partition).truncateTo(futureReplicaLEO, isFuture = true)
  }

  @Test
  def shouldFetchOneReplicaAtATime(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    //Stubs
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("Y0qUL19gSmKAXmohmrUM4g")))
    stub(log, null, futureLog, partition, replicaManager)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.leader.buildFetch(Map(
      t1p0 -> PartitionFetchState(Some(topicId), 150, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = Optional.empty),
      t1p1 -> PartitionFetchState(Some(topicId), 160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = Optional.empty)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get.fetchRequest
    assertFalse(fetchRequest.fetchData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val request = fetchRequest.build()
    assertEquals(0, request.minBytes)
    val fetchInfos = request.fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1.topicPartition, "Expected fetch request for first partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)
  }

  @Test
  def shouldFetchNonDelayedAndNonTruncatingReplicas(): Unit = {

    //Setup all dependencies
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val quotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val futureLog: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    //Stubs
    val startOffset = 123
    when(futureLog.logStartOffset).thenReturn(startOffset)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(partition.logDirectoryId()).thenReturn(Some(Uuid.fromString("rtrdy3nsQwO1OQUEUYGxRQ")))
    stub(log, null, futureLog, partition, replicaManager)

    //Create the fetcher thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val leaderEpoch = 1
    val leader = new LocalLeaderEndPoint(endPoint, config, replicaManager, quotaManager)
    val thread = new ReplicaAlterLogDirsThread(
      "alter-logs-dirs-thread-test1",
      leader,
      failedPartitions,
      replicaManager,
      quotaManager,
      null,
      config.replicaFetchBackoffMs)
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(0L, leaderEpoch),
      t1p1 -> initialFetchState(0L, leaderEpoch)))

    // one partition is ready and one is truncating
    val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = thread.leader.buildFetch(Map(
        t1p0 -> PartitionFetchState(Some(topicId), 150, None, leaderEpoch, state = Fetching, lastFetchedEpoch = Optional.empty),
        t1p1 -> PartitionFetchState(Some(topicId), 160, None, leaderEpoch, state = Truncating, lastFetchedEpoch = Optional.empty)))

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequest = fetchRequestOpt.get
    assertFalse(fetchRequest.partitionData.isEmpty)
    assertFalse(partitionsWithError.nonEmpty)
    val fetchInfos = fetchRequest.fetchRequest.build().fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos.length)
    assertEquals(t1p0, fetchInfos.head._1.topicPartition, "Expected fetch request for non-truncating partition")
    assertEquals(150, fetchInfos.head._2.fetchOffset)

    // one partition is ready and one is delayed
    val ResultWithPartitions(fetchRequest2Opt, partitionsWithError2) = thread.leader.buildFetch(Map(
        t1p0 -> PartitionFetchState(Some(topicId), 140, None, leaderEpoch, state = Fetching, lastFetchedEpoch = Optional.empty),
        t1p1 -> PartitionFetchState(Some(topicId), 160, None, leaderEpoch, delay = Some(5000), state = Fetching, lastFetchedEpoch = Optional.empty)))

    assertTrue(fetchRequest2Opt.isDefined)
    val fetchRequest2 = fetchRequest2Opt.get
    assertFalse(fetchRequest2.partitionData.isEmpty)
    assertFalse(partitionsWithError2.nonEmpty)
    val fetchInfos2 = fetchRequest2.fetchRequest.build().fetchData(topicNames.asJava).asScala.toSeq
    assertEquals(1, fetchInfos2.length)
    assertEquals(t1p0, fetchInfos2.head._1.topicPartition, "Expected fetch request for non-delayed partition")
    assertEquals(140, fetchInfos2.head._2.fetchOffset)

    // both partitions are delayed
    val ResultWithPartitions(fetchRequest3Opt, partitionsWithError3) = thread.leader.buildFetch(Map(
        t1p0 -> PartitionFetchState(Some(topicId), 140, None, leaderEpoch, delay = Some(5000), state = Fetching, lastFetchedEpoch = Optional.empty),
        t1p1 -> PartitionFetchState(Some(topicId), 160, None, leaderEpoch, delay = Some(5000), state = Fetching, lastFetchedEpoch = Optional.empty)))
    assertTrue(fetchRequest3Opt.isEmpty, "Expected no fetch requests since all partitions are delayed")
    assertFalse(partitionsWithError3.nonEmpty)
  }

  def stub(logT1p0: UnifiedLog, logT1p1: UnifiedLog, futureLog: UnifiedLog, partition: Partition,
           replicaManager: ReplicaManager): Unit = {
    when(replicaManager.localLog(t1p0)).thenReturn(Some(logT1p0))
    when(replicaManager.localLogOrException(t1p0)).thenReturn(logT1p0)
    when(replicaManager.futureLocalLogOrException(t1p0)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p0)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p0)).thenReturn(Some(partition))
    when(replicaManager.localLog(t1p1)).thenReturn(Some(logT1p1))
    when(replicaManager.localLogOrException(t1p1)).thenReturn(logT1p1)
    when(replicaManager.futureLocalLogOrException(t1p1)).thenReturn(futureLog)
    when(replicaManager.futureLogExists(t1p1)).thenReturn(true)
    when(replicaManager.onlinePartition(t1p1)).thenReturn(Some(partition))
  }

  def stubWithFetchMessages(logT1p0: UnifiedLog, logT1p1: UnifiedLog, futureLog: UnifiedLog, partition: Partition, replicaManager: ReplicaManager,
                            responseCallback: ArgumentCaptor[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]): Unit = {
    stub(logT1p0, logT1p1, futureLog, partition, replicaManager)
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      responseCallback.capture()
    )).thenAnswer(_ => responseCallback.getValue.apply(Seq.empty[(TopicIdPartition, FetchPartitionData)]))
  }
}
