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
package kafka.server.epoch

import java.io.File
import kafka.log.LogManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.server.metadata.KRaftMetadataCache
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.server.common.{KRaftVersion, OffsetAndEpoch}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{LogDirFailureChannel, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.{mock, when}

import java.util.Optional
import scala.jdk.CollectionConverters._

class OffsetsForLeaderEpochTest {
  private val config = TestUtils.createBrokerConfigs(1).map(KafkaConfig.fromProps).head
  private val time = new MockTime
  private val metrics = new Metrics
  private val alterIsrManager = TestUtils.createAlterIsrManager()
  private val tp = new TopicPartition("topic", 1)
  private var replicaManager: ReplicaManager = _
  private var quotaManager: QuotaManagers = _

  @BeforeEach
  def setUp(): Unit = {
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "", "")
  }

  @Test
  def shouldGetEpochsFromReplica(): Unit = {
    //Given
    val offsetAndEpoch = new OffsetAndEpoch(42L, 5)
    val epochRequested: Integer = 5
    val request = Seq(newOffsetForLeaderTopic(tp, RecordBatch.NO_PARTITION_LEADER_EPOCH, epochRequested))

    //Stubs
    val mockLog: UnifiedLog = mock(classOf[UnifiedLog])
    val logManager: LogManager = mock(classOf[LogManager])
    when(mockLog.endOffsetForEpoch(epochRequested)).thenReturn(Optional.of(offsetAndEpoch))
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    // create a replica manager with 1 partition that has 1 replica
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = null,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterIsrManager)
    val partition = replicaManager.createPartition(tp)
    partition.setLog(mockLog, isFutureLog = false)
    partition.leaderReplicaIdOpt = Some(config.brokerId)

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(
      Seq(newOffsetForLeaderTopicResult(tp, Errors.NONE, offsetAndEpoch.epoch(), offsetAndEpoch.offset)),
      response)
  }

  @Test
  def shouldReturnNoLeaderForPartitionIfThrown(): Unit = {
    val logManager: LogManager = mock(classOf[LogManager])
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    //create a replica manager with 1 partition that has 0 replica
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = null,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterIsrManager)
    replicaManager.createPartition(tp)

    //Given
    val epochRequested: Integer = 5
    val request = Seq(newOffsetForLeaderTopic(tp, RecordBatch.NO_PARTITION_LEADER_EPOCH, epochRequested))

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(
      Seq(newOffsetForLeaderTopicResult(tp, Errors.NOT_LEADER_OR_FOLLOWER, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)),
      response)
  }

  @Test
  def shouldReturnUnknownTopicOrPartitionIfThrown(): Unit = {
    val logManager: LogManager = mock(classOf[LogManager])
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    //create a replica manager with 0 partition
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = null,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = new KRaftMetadataCache(config.brokerId, () => KRaftVersion.KRAFT_VERSION_0),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = alterIsrManager)

    //Given
    val epochRequested: Integer = 5
    val request = Seq(newOffsetForLeaderTopic(tp, RecordBatch.NO_PARTITION_LEADER_EPOCH, epochRequested))

    //When
    val response = replicaManager.lastOffsetForLeaderEpoch(request)

    //Then
    assertEquals(
      Seq(newOffsetForLeaderTopicResult(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)),
      response)
  }

  @AfterEach
  def tearDown(): Unit = {
    Option(replicaManager).foreach(_.shutdown(checkpointHW = false))
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  private def newOffsetForLeaderTopic(
    tp: TopicPartition,
    currentLeaderEpoch: Int,
    leaderEpoch: Int
  ): OffsetForLeaderTopic = {
    new OffsetForLeaderTopic()
      .setTopic(tp.topic)
      .setPartitions(List(new OffsetForLeaderPartition()
        .setPartition(tp.partition)
        .setCurrentLeaderEpoch(currentLeaderEpoch)
        .setLeaderEpoch(leaderEpoch)).asJava)
  }

  private def newOffsetForLeaderTopicResult(
     tp: TopicPartition,
     error: Errors,
     leaderEpoch: Int,
     endOffset: Long
  ): OffsetForLeaderTopicResult = {
    new OffsetForLeaderTopicResult()
      .setTopic(tp.topic)
      .setPartitions(List(new EpochEndOffset()
        .setPartition(tp.partition)
        .setErrorCode(error.code)
        .setLeaderEpoch(leaderEpoch)
        .setEndOffset(endOffset)).asJava)
  }
}
