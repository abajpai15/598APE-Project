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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
  )
)
class OffsetDeleteRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testOffsetDeleteWithNewConsumerGroupProtocol(): Unit = {
    testOffsetDelete(true)
  }

  @ClusterTest
  def testOffsetDeleteWithOldConsumerGroupProtocol(): Unit = {
    testOffsetDelete(false)
  }

  private def testOffsetDelete(useNewProtocol: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    val topicId = createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.OFFSET_DELETE.oldestVersion() to ApiKeys.OFFSET_DELETE.latestVersion(isUnstableApiEnabled)) {
      // Join the consumer group. Note that we don't heartbeat here so we must use
      // a session long enough for the duration of the test.
      val (memberId, memberEpoch) = joinConsumerGroup(
        groupId = "grp",
        useNewProtocol = useNewProtocol
      )

      // Commit offsets.
      for (partitionId <- 0 to 2) {
        commitOffset(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          topic = "foo",
          topicId = topicId,
          partition = partitionId,
          offset = 100L + partitionId,
          expectedError = Errors.NONE,
          version = ApiKeys.OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)
        )
      }

      // Delete offset with topic that the group is subscribed to.
      deleteOffset(
        groupId = "grp",
        topic = "foo",
        partition = 0,
        expectedPartitionError = Errors.GROUP_SUBSCRIBED_TO_TOPIC,
        version = version.toShort
      )

      // Unsubscribe the topic.
      if (useNewProtocol) {
        consumerGroupHeartbeat(
          groupId = "grp",
          memberId = memberId,
          memberEpoch = memberEpoch,
          subscribedTopicNames = List.empty
        )
      } else {
        leaveGroup(
          groupId = "grp",
          memberId = memberId,
          useNewProtocol = false,
          version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
        )
      }

      // Delete offsets.
      for (partitionId <- 0 to 1) {
        deleteOffset(
          groupId = "grp",
          topic = "foo",
          partition = partitionId,
          version = version.toShort
        )
      }

      // Delete offsets with partition that doesn't exist.
      deleteOffset(
        groupId = "grp",
        topic = "foo",
        partition = 5,
        expectedPartitionError = Errors.UNKNOWN_TOPIC_OR_PARTITION,
        version = version.toShort
      )

      // Delete offset with unknown group id.
      deleteOffset(
        groupId = "grp-unknown",
        topic = "foo",
        partition = 2,
        expectedResponseError = Errors.GROUP_ID_NOT_FOUND,
        version = version.toShort
      )

      // Delete offset with empty group id.
      deleteOffset(
        groupId = "",
        topic = "foo",
        partition = 2,
        expectedResponseError = Errors.INVALID_GROUP_ID,
        version = version.toShort
      )

      // Delete offset with both invalid id and invalid topic should return with a response with top-level error.
      deleteOffset(
        groupId = "",
        topic = "foo-unknown",
        partition = 2,
        expectedResponseError = Errors.INVALID_GROUP_ID,
        version = version.toShort
      )

      // Delete offset with both invalid id and invalid partition should return with a response with top-level error.
      deleteOffset(
        groupId = "",
        topic = "foo",
        partition = 5,
        expectedResponseError = Errors.INVALID_GROUP_ID,
        version = version.toShort
      )
    }
  }
}
