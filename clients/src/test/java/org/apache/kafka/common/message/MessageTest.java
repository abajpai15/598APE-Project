/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.message;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
public final class MessageTest {

    private final String memberId = "memberId";
    private final String instanceId = "instanceId";

    @Test
    public void testAddOffsetsToTxnVersions() throws Exception {
        testAllMessageRoundTrips(new AddOffsetsToTxnRequestData().
                setTransactionalId("foobar").
                setProducerId(0xbadcafebadcafeL).
                setProducerEpoch((short) 123).
                setGroupId("baaz"));
        testAllMessageRoundTrips(new AddOffsetsToTxnResponseData().
                setThrottleTimeMs(42).
                setErrorCode((short) 0));
    }

    @Test
    public void testAddPartitionsToTxnVersions() throws Exception {
        AddPartitionsToTxnRequestData v3AndBelowData = new AddPartitionsToTxnRequestData().
                setV3AndBelowTransactionalId("blah").
                setV3AndBelowProducerId(0xbadcafebadcafeL).
                setV3AndBelowProducerEpoch((short) 30000).
                setV3AndBelowTopics(new AddPartitionsToTxnTopicCollection(singletonList(
                        new AddPartitionsToTxnTopic().
                                setName("Topic").
                                setPartitions(singletonList(1))).iterator()));
        testDuplication(v3AndBelowData);
        testAllMessageRoundTripsUntilVersion((short) 3, v3AndBelowData);

        AddPartitionsToTxnRequestData data = new AddPartitionsToTxnRequestData().
                setTransactions(new AddPartitionsToTxnTransactionCollection(singletonList(
                       new AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction().
                              setTransactionalId("blah").
                              setProducerId(0xbadcafebadcafeL).
                              setProducerEpoch((short) 30000).
                              setTopics(v3AndBelowData.v3AndBelowTopics())).iterator()));
        testDuplication(data);
        testAllMessageRoundTripsFromVersion((short) 4, data);
    }

    @Test
    public void testCreateTopicsVersions() throws Exception {
        testAllMessageRoundTrips(new CreateTopicsRequestData().
                setTimeoutMs(1000).setTopics(new CreateTopicsRequestData.CreatableTopicCollection()));
    }

    @Test
    public void testDescribeAclsRequest() throws Exception {
        testAllMessageRoundTrips(new DescribeAclsRequestData().
                setResourceTypeFilter((byte) 42).
                setResourceNameFilter(null).
                setPatternTypeFilter((byte) 3).
                setPrincipalFilter("abc").
                setHostFilter(null).
                setOperation((byte) 0).
                setPermissionType((byte) 0));
    }

    @Test
    public void testMetadataVersions() throws Exception {
        testAllMessageRoundTrips(new MetadataRequestData().setTopics(
                Arrays.asList(new MetadataRequestData.MetadataRequestTopic().setName("foo"),
                        new MetadataRequestData.MetadataRequestTopic().setName("bar")
                )));
        testAllMessageRoundTripsFromVersion((short) 1, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(true).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
        testAllMessageRoundTripsFromVersion((short) 4, new MetadataRequestData().
                setTopics(null).
                setAllowAutoTopicCreation(false).
                setIncludeClusterAuthorizedOperations(false).
                setIncludeTopicAuthorizedOperations(false));
    }

    @Test
    public void testHeartbeatVersions() throws Exception {
        Supplier<HeartbeatRequestData> newRequest = () -> new HeartbeatRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testJoinGroupRequestVersions() throws Exception {
        Supplier<JoinGroupRequestData> newRequest = () -> new JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setProtocolType("consumer")
                .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection())
                .setSessionTimeoutMs(10000);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 1, newRequest.get().setRebalanceTimeoutMs(20000));
        testAllMessageRoundTrips(newRequest.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newRequest.get().setGroupInstanceId("instanceId"));
    }

    @Test
    public void testListOffsetsRequestVersions() throws Exception {
        List<ListOffsetsTopic> v = Collections.singletonList(new ListOffsetsTopic()
                .setName("topic")
                .setPartitions(Collections.singletonList(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(123L))));
        Supplier<ListOffsetsRequestData> newRequest = () -> new ListOffsetsRequestData()
                .setTopics(v)
                .setReplicaId(0);
        testAllMessageRoundTrips(newRequest.get());
        testAllMessageRoundTripsFromVersion((short) 2, newRequest.get().setIsolationLevel(IsolationLevel.READ_COMMITTED.id()));
    }

    @Test
    public void testListOffsetsResponseVersions() throws Exception {
        ListOffsetsPartitionResponse partition = new ListOffsetsPartitionResponse()
                .setErrorCode(Errors.NONE.code())
                .setPartitionIndex(0);
        List<ListOffsetsTopicResponse> topics = Collections.singletonList(new ListOffsetsTopicResponse()
                .setName("topic")
                .setPartitions(Collections.singletonList(partition)));
        Supplier<ListOffsetsResponseData> response = () -> new ListOffsetsResponseData()
                .setTopics(topics);
        for (short version = ApiKeys.LIST_OFFSETS.oldestVersion(); version <= ApiKeys.LIST_OFFSETS.latestVersion(); ++version) {
            ListOffsetsResponseData responseData = response.get();
            responseData.topics().get(0).partitions().get(0)
                .setOffset(456L)
                .setTimestamp(123L);
            if (version > 1) {
                responseData.setThrottleTimeMs(1000);
            }
            if (version > 3) {
                partition.setLeaderEpoch(1);
            }
            testEquivalentMessageRoundTrip(version, responseData);
        }
    }

    @Test
    public void testJoinGroupResponseVersions() throws Exception {
        Supplier<JoinGroupResponseData> newResponse = () -> new JoinGroupResponseData()
                .setMemberId(memberId)
                .setLeader(memberId)
                .setGenerationId(1)
                .setMembers(Collections.singletonList(
                        new JoinGroupResponseMember()
                                .setMemberId(memberId)
                ));
        testAllMessageRoundTrips(newResponse.get());
        testAllMessageRoundTripsFromVersion((short) 2, newResponse.get().setThrottleTimeMs(1000));
        testAllMessageRoundTrips(newResponse.get().members().get(0).setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 5, newResponse.get().members().get(0).setGroupInstanceId("instanceId"));
    }

    @Test
    public void testLeaveGroupResponseVersions() throws Exception {
        Supplier<LeaveGroupResponseData> newResponse = () -> new LeaveGroupResponseData()
                                                                 .setErrorCode(Errors.NOT_COORDINATOR.code());

        testAllMessageRoundTrips(newResponse.get());
        testAllMessageRoundTripsFromVersion((short) 1, newResponse.get().setThrottleTimeMs(1000));

        testAllMessageRoundTripsFromVersion((short) 3, newResponse.get().setMembers(
            Collections.singletonList(new MemberResponse()
            .setMemberId(memberId)
            .setGroupInstanceId(instanceId))
        ));
    }

    @Test
    public void testSyncGroupDefaultGroupInstanceId() throws Exception {
        Supplier<SyncGroupRequestData> request = () -> new SyncGroupRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setGenerationId(15)
                .setAssignments(new ArrayList<>());
        testAllMessageRoundTrips(request.get());
        testAllMessageRoundTrips(request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 3, request.get().setGroupInstanceId(instanceId));
    }

    @Test
    public void testOffsetCommitDefaultGroupInstanceId() throws Exception {
        testAllMessageRoundTrips(new OffsetCommitRequestData()
                .setTopics(new ArrayList<>())
                .setGroupId("groupId"));

        Supplier<OffsetCommitRequestData> request = () -> new OffsetCommitRequestData()
                .setGroupId("groupId")
                .setMemberId(memberId)
                .setTopics(new ArrayList<>())
                .setGenerationIdOrMemberEpoch(15);
        testAllMessageRoundTripsFromVersion((short) 1, request.get());
        testAllMessageRoundTripsFromVersion((short) 1, request.get().setGroupInstanceId(null));
        testAllMessageRoundTripsFromVersion((short) 7, request.get().setGroupInstanceId(instanceId));
    }

    @Test
    public void testDescribeGroupsRequestVersions() throws Exception {
        testAllMessageRoundTrips(new DescribeGroupsRequestData()
                .setGroups(Collections.singletonList("group"))
                .setIncludeAuthorizedOperations(false));
    }

    @Test
    public void testDescribeGroupsResponseVersions() throws Exception {
        DescribedGroupMember baseMember = new DescribedGroupMember()
            .setMemberId(memberId);

        DescribedGroup baseGroup = new DescribedGroup()
                                       .setGroupId("group")
                                       .setGroupState("Stable").setErrorCode(Errors.NONE.code())
                                       .setMembers(Collections.singletonList(baseMember))
                                       .setProtocolType("consumer");
        DescribeGroupsResponseData baseResponse = new DescribeGroupsResponseData()
                                                      .setGroups(Collections.singletonList(baseGroup));
        testAllMessageRoundTrips(baseResponse);

        testAllMessageRoundTripsFromVersion((short) 1, baseResponse.setThrottleTimeMs(10));

        baseGroup.setAuthorizedOperations(1);
        testAllMessageRoundTripsFromVersion((short) 3, baseResponse);

        baseMember.setGroupInstanceId(instanceId);
        testAllMessageRoundTripsFromVersion((short) 4, baseResponse);
    }

    @Test
    public void testDescribeClusterRequestVersions() throws Exception {
        testAllMessageRoundTrips(new DescribeClusterRequestData()
            .setIncludeClusterAuthorizedOperations(true));
    }

    @Test
    public void testDescribeClusterResponseVersions() throws Exception {
        DescribeClusterResponseData data = new DescribeClusterResponseData()
            .setBrokers(new DescribeClusterBrokerCollection(
                Collections.singletonList(new DescribeClusterBroker()
                    .setBrokerId(1)
                    .setHost("localhost")
                    .setPort(9092)
                    .setRack("rack1")).iterator()))
            .setClusterId("clusterId")
            .setControllerId(1)
            .setClusterAuthorizedOperations(10);

        testAllMessageRoundTrips(data);
    }

    @Test
    public void testGroupInstanceIdIgnorableInDescribeGroupsResponse() throws Exception {
        DescribeGroupsResponseData responseWithGroupInstanceId =
            new DescribeGroupsResponseData()
                .setGroups(Collections.singletonList(
                    new DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code())
                        .setMembers(Collections.singletonList(
                            new DescribedGroupMember()
                                .setMemberId(memberId)
                                .setGroupInstanceId(instanceId)))
                        .setProtocolType("consumer")
                ));

        DescribeGroupsResponseData expectedResponse = responseWithGroupInstanceId.duplicate();
        // Unset GroupInstanceId
        expectedResponse.groups().get(0).members().get(0).setGroupInstanceId(null);

        testAllMessageRoundTripsBeforeVersion((short) 4, responseWithGroupInstanceId, expectedResponse);
    }

    @Test
    public void testThrottleTimeIgnorableInDescribeGroupsResponse() throws Exception {
        DescribeGroupsResponseData responseWithGroupInstanceId =
            new DescribeGroupsResponseData()
                .setGroups(Collections.singletonList(
                    new DescribedGroup()
                        .setGroupId("group")
                        .setGroupState("Stable")
                        .setErrorCode(Errors.NONE.code())
                        .setMembers(Collections.singletonList(
                            new DescribedGroupMember()
                                .setMemberId(memberId)))
                        .setProtocolType("consumer")
                ))
                .setThrottleTimeMs(10);

        DescribeGroupsResponseData expectedResponse = responseWithGroupInstanceId.duplicate();
        // Unset throttle time
        expectedResponse.setThrottleTimeMs(0);

        testAllMessageRoundTripsBeforeVersion((short) 1, responseWithGroupInstanceId, expectedResponse);
    }

    @Test
    public void testOffsetForLeaderEpochVersions() throws Exception {
        // Version 2 adds optional current leader epoch
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataNoCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartition(0)
                        .setLeaderEpoch(3);
        OffsetForLeaderEpochRequestData.OffsetForLeaderPartition partitionDataWithCurrentEpoch =
                new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                        .setPartition(0)
                        .setLeaderEpoch(3)
                        .setCurrentLeaderEpoch(5);
        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData();
        data.topics().add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
                .setTopic("foo")
                .setPartitions(singletonList(partitionDataNoCurrentEpoch)));

        testAllMessageRoundTrips(data);
        short lowestVersion = ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion();
        testAllMessageRoundTripsBetweenVersions(lowestVersion, (short) 2, partitionDataWithCurrentEpoch, partitionDataNoCurrentEpoch);
        testAllMessageRoundTripsFromVersion((short) 2, partitionDataWithCurrentEpoch);

        // Version 3 adds the optional replica Id field
        testAllMessageRoundTripsFromVersion((short) 3, new OffsetForLeaderEpochRequestData().setReplicaId(5));
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData());
        testAllMessageRoundTripsBeforeVersion((short) 3,
                new OffsetForLeaderEpochRequestData().setReplicaId(5),
                new OffsetForLeaderEpochRequestData().setReplicaId(-2));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitRequestVersions(short version) throws Exception {
        OffsetCommitRequestData request = new OffsetCommitRequestData()
            .setGroupId("groupId")
            .setMemberId("memberId")
            .setGenerationIdOrMemberEpoch(version >= 1 ? 10 : -1)
            .setGroupInstanceId(version >= 7 ? "instanceId" : null)
            .setRetentionTimeMs((version >= 2 && version <= 4) ? 20 : -1)
            .setTopics(singletonList(
                new OffsetCommitRequestTopic()
                    .setTopicId(version >= 10 ? Uuid.randomUuid() : Uuid.ZERO_UUID)
                    .setName(version < 10 ? "topic" : "")
                    .setPartitions(singletonList(
                        new OffsetCommitRequestPartition()
                            .setPartitionIndex(1)
                            .setCommittedMetadata("metadata")
                            .setCommittedOffset(100)
                            .setCommittedLeaderEpoch(version >= 6 ? 10 : -1)

                    ))
            ));

        testMessageRoundTrip(version, request, request);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitResponseVersions(short version) throws Exception {
        OffsetCommitResponseData response = new OffsetCommitResponseData()
            .setThrottleTimeMs(version >= 3 ? 20 : 0)
            .setTopics(singletonList(
                new OffsetCommitResponseTopic()
                    .setTopicId(version >= 10 ? Uuid.randomUuid() : Uuid.ZERO_UUID)
                    .setName(version < 10 ? "topic" : "")
                    .setPartitions(singletonList(
                        new OffsetCommitResponsePartition()
                            .setPartitionIndex(1)
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                    ))
            ));

        testMessageRoundTrip(version, response, response);
    }

    @Test
    public void testTxnOffsetCommitRequestVersions() throws Exception {
        String groupId = "groupId";
        String topicName = "topic";
        String metadata = "metadata";
        String txnId = "transactionalId";
        int producerId = 25;
        short producerEpoch = 10;
        String instanceId = "instance";
        String memberId = "member";
        int generationId = 1;

        int partition = 2;
        int offset = 100;

        testAllMessageRoundTrips(new TxnOffsetCommitRequestData()
                                     .setGroupId(groupId)
                                     .setTransactionalId(txnId)
                                     .setProducerId(producerId)
                                     .setProducerEpoch(producerEpoch)
                                     .setTopics(Collections.singletonList(
                                         new TxnOffsetCommitRequestTopic()
                                             .setName(topicName)
                                             .setPartitions(Collections.singletonList(
                                                 new TxnOffsetCommitRequestPartition()
                                                     .setPartitionIndex(partition)
                                                     .setCommittedMetadata(metadata)
                                                     .setCommittedOffset(offset)
                                             )))));

        Supplier<TxnOffsetCommitRequestData> request =
            () -> new TxnOffsetCommitRequestData()
                      .setGroupId(groupId)
                      .setTransactionalId(txnId)
                      .setProducerId(producerId)
                      .setProducerEpoch(producerEpoch)
                      .setGroupInstanceId(instanceId)
                      .setMemberId(memberId)
                      .setGenerationId(generationId)
                      .setTopics(Collections.singletonList(
                          new TxnOffsetCommitRequestTopic()
                              .setName(topicName)
                              .setPartitions(Collections.singletonList(
                                  new TxnOffsetCommitRequestPartition()
                                      .setPartitionIndex(partition)
                                      .setCommittedLeaderEpoch(10)
                                      .setCommittedMetadata(metadata)
                                      .setCommittedOffset(offset)
                              ))));

        for (short version : ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            TxnOffsetCommitRequestData requestData = request.get();
            if (version < 2) {
                requestData.topics().get(0).partitions().get(0).setCommittedLeaderEpoch(-1);
            }

            if (version < 3) {
                final short finalVersion = version;
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setGroupInstanceId(null);
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setMemberId("");
                assertThrows(UnsupportedVersionException.class, () -> testEquivalentMessageRoundTrip(finalVersion, requestData));
                requestData.setGenerationId(-1);
            }

            testAllMessageRoundTripsFromVersion(version, requestData);
        }
    }

    @Test
    public void testTxnOffsetCommitResponseVersions() throws Exception {
        testAllMessageRoundTrips(
            new TxnOffsetCommitResponseData()
                .setTopics(
                   singletonList(
                       new TxnOffsetCommitResponseTopic()
                           .setName("topic")
                           .setPartitions(singletonList(
                               new TxnOffsetCommitResponsePartition()
                                   .setPartitionIndex(1)
                                   .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                           ))
                   )
               )
               .setThrottleTimeMs(20));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testOffsetFetchRequestVersions(short version) throws Exception {
        OffsetFetchRequestData request;

        if (version < 8) {
            request = new OffsetFetchRequestData()
                .setGroupId("groupId")
                .setRequireStable(version == 7)
                .setTopics(List.of(
                    new OffsetFetchRequestTopic()
                        .setName("foo")
                        .setPartitionIndexes(List.of(0, 1, 2))
                ));
        } else {
            request = new OffsetFetchRequestData()
                .setRequireStable(true)
                .setGroups(List.of(
                    new OffsetFetchRequestGroup()
                        .setGroupId("groupId")
                        .setMemberId(version >= 9 ? "memberId" : null)
                        .setMemberEpoch(version >= 9 ? 10 : -1)
                        .setTopics(List.of(
                            new OffsetFetchRequestTopics()
                                .setName("foo")
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                ));
        }

        testMessageRoundTrip(version, request, request);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testOffsetFetchResponseVersions(short version) throws Exception {
        OffsetFetchResponseData response;

        if (version < 8) {
            response = new OffsetFetchResponseData()
                .setThrottleTimeMs(version >= 3 ? 1000 : 0)
                .setErrorCode(version >= 2 ? Errors.INVALID_GROUP_ID.code() : 0)
                .setTopics(List.of(
                    new OffsetFetchResponseTopic()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponsePartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setMetadata("meta")
                                .setCommittedLeaderEpoch(version >= 5 ? 20 : -1)
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        ))
                ));
        } else {
            response = new OffsetFetchResponseData()
                .setThrottleTimeMs(1000)
                .setGroups(List.of(
                    new OffsetFetchResponseGroup()
                        .setGroupId("groupId")
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                        .setTopics(List.of(
                            new OffsetFetchResponseTopics()
                                .setName("foo")
                                .setPartitions(List.of(
                                    new OffsetFetchResponsePartitions()
                                        .setPartitionIndex(0)
                                        .setCommittedOffset(10)
                                        .setMetadata("meta")
                                        .setCommittedLeaderEpoch(20)
                                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                ))
                        ))
                ));
        }

        testMessageRoundTrip(version, response, response);
    }

    @Test
    public void testProduceResponseVersions() throws Exception {
        String topicName = "topic";
        Uuid topicId = Uuid.fromString("klZ9sa2rSvig6QpgGXzALT");

        int partitionIndex = 0;
        short errorCode = Errors.INVALID_TOPIC_EXCEPTION.code();
        long baseOffset = 12L;
        int throttleTimeMs = 1234;
        long logAppendTimeMs = 1234L;
        long logStartOffset = 1234L;
        int batchIndex = 0;
        String batchIndexErrorMessage = "error message";
        String errorMessage = "global error message";

        testAllMessageRoundTrips(new ProduceResponseData()
            .setResponses(new ProduceResponseData.TopicProduceResponseCollection(singletonList(
                new ProduceResponseData.TopicProduceResponse()
                    .setPartitionResponses(singletonList(
                        new ProduceResponseData.PartitionProduceResponse()
                            .setIndex(partitionIndex)
                            .setErrorCode(errorCode)
                            .setBaseOffset(baseOffset)))).iterator())));

        Supplier<ProduceResponseData> response = () -> new ProduceResponseData()
                .setResponses(new ProduceResponseData.TopicProduceResponseCollection(singletonList(
                    new ProduceResponseData.TopicProduceResponse()
                        .setPartitionResponses(singletonList(
                             new ProduceResponseData.PartitionProduceResponse()
                                 .setIndex(partitionIndex)
                                 .setErrorCode(errorCode)
                                 .setBaseOffset(baseOffset)
                                 .setLogAppendTimeMs(logAppendTimeMs)
                                 .setLogStartOffset(logStartOffset)
                                 .setRecordErrors(singletonList(
                                     new ProduceResponseData.BatchIndexAndErrorMessage()
                                         .setBatchIndex(batchIndex)
                                         .setBatchIndexErrorMessage(batchIndexErrorMessage)))
                                 .setErrorMessage(errorMessage)))).iterator()))
                .setThrottleTimeMs(throttleTimeMs);

        for (short version : ApiKeys.PRODUCE.allVersions()) {
            ProduceResponseData responseData = response.get();

            if (version < 8) {
                responseData.responses().iterator().next().partitionResponses().get(0).setRecordErrors(Collections.emptyList());
                responseData.responses().iterator().next().partitionResponses().get(0).setErrorMessage(null);
            }

            if (version < 5) {
                responseData.responses().iterator().next().partitionResponses().get(0).setLogStartOffset(-1);
            }

            if (version < 2) {
                responseData.responses().iterator().next().partitionResponses().get(0).setLogAppendTimeMs(-1);
            }

            if (version < 1) {
                responseData.setThrottleTimeMs(0);
            }

            if (version >= 13) {
                responseData.responses().iterator().next().setTopicId(topicId);
            } else {
                responseData.responses().iterator().next().setName(topicName);
            }

            if (version >= 3 && version <= 4) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 5, responseData, responseData);
            } else if (version >= 6 && version <= 7) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 8, responseData, responseData);
            } else if (version <= 12) {
                testAllMessageRoundTripsBetweenVersions(version, (short) 12, responseData, responseData);
            } else {
                testEquivalentMessageRoundTrip(version, responseData);
            }
        }
    }

    @Test
    public void defaultValueShouldBeWritable() {
        for (short version = SimpleExampleMessageData.LOWEST_SUPPORTED_VERSION; version <= SimpleExampleMessageData.HIGHEST_SUPPORTED_VERSION; ++version) {
            MessageUtil.toByteBufferAccessor(new SimpleExampleMessageData(), version).buffer();
        }
    }

    @Test
    public void testSimpleMessage() throws Exception {
        final SimpleExampleMessageData message = new SimpleExampleMessageData();
        message.setMyStruct(new SimpleExampleMessageData.MyStruct().setStructId(25).setArrayInStruct(
            Collections.singletonList(new SimpleExampleMessageData.StructArray().setArrayFieldId(20))
        ));
        message.setMyTaggedStruct(new SimpleExampleMessageData.TaggedStruct().setStructId("abc"));

        message.setProcessId(Uuid.randomUuid());
        message.setMyNullableString("notNull");
        message.setMyInt16((short) 3);
        message.setMyString("test string");
        SimpleExampleMessageData duplicate = message.duplicate();
        assertEquals(duplicate, message);
        assertEquals(message, duplicate);
        duplicate.setMyTaggedIntArray(Collections.singletonList(123));
        assertNotEquals(duplicate, message);
        assertNotEquals(message, duplicate);

        testAllMessageRoundTripsFromVersion((short) 2, message);
    }

    private void testAllMessageRoundTrips(Message message) throws Exception {
        testDuplication(message);
        testAllMessageRoundTripsFromVersion(message.lowestSupportedVersion(), message);
    }

    private void testDuplication(Message message) {
        Message duplicate = message.duplicate();
        assertEquals(duplicate, message);
        assertEquals(message, duplicate);
        assertEquals(duplicate.hashCode(), message.hashCode());
        assertEquals(message.hashCode(), duplicate.hashCode());
    }

    private void testAllMessageRoundTripsBeforeVersion(short beforeVersion, Message message, Message expected) throws Exception {
        testAllMessageRoundTripsBetweenVersions((short) 0, beforeVersion, message, expected);
    }

    /**
     * @param startVersion - the version we want to start at, inclusive
     * @param endVersion - the version we want to end at, exclusive
     */
    private void testAllMessageRoundTripsBetweenVersions(short startVersion, short endVersion, Message message, Message expected) throws Exception {
        for (short version = startVersion; version < endVersion; version++) {
            testMessageRoundTrip(version, message, expected);
        }
    }

    private void testAllMessageRoundTripsFromVersion(short fromVersion, Message message) throws Exception {
        for (short version = fromVersion; version <= message.highestSupportedVersion(); version++) {
            testEquivalentMessageRoundTrip(version, message);
        }
    }

    private void testAllMessageRoundTripsUntilVersion(short untilVersion, Message message) throws Exception {
        for (short version = message.lowestSupportedVersion(); version <= untilVersion; version++) {
            testEquivalentMessageRoundTrip(version, message);
        }
    }

    private void testMessageRoundTrip(short version, Message message, Message expected) throws Exception {
        testByteBufferRoundTrip(version, message, expected);
    }

    private void testEquivalentMessageRoundTrip(short version, Message message) throws Exception {
        testByteBufferRoundTrip(version, message, message);
        testJsonRoundTrip(version, message, message);
    }

    private void testByteBufferRoundTrip(short version, Message message, Message expected) throws Exception {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals(size, buf.position(), "The result of the size function does not match the number of bytes " +
            "written for version " + version);
        Message message2 = message.getClass().getConstructor().newInstance();
        buf.flip();
        message2.read(byteBufferAccessor, version);
        assertEquals(size, buf.position(), "The result of the size function does not match the number of bytes " +
            "read back in for version " + version);
        assertEquals(expected, message2, "The message object created after a round trip did not match for " +
            "version " + version);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    private void testJsonRoundTrip(short version, Message message, Message expected) throws Exception {
        String jsonConverter = jsonConverterTypeName(message.getClass().getTypeName());
        Class<?> converter = Class.forName(jsonConverter);
        Method writeMethod = converter.getMethod("write", message.getClass(), short.class);
        JsonNode jsonNode = (JsonNode) writeMethod.invoke(null, message, version);
        Method readMethod = converter.getMethod("read", JsonNode.class, short.class);
        Message message2 = (Message) readMethod.invoke(null, jsonNode, version);
        assertEquals(expected, message2);
        assertEquals(expected.hashCode(), message2.hashCode());
        assertEquals(expected.toString(), message2.toString());
    }

    private static String jsonConverterTypeName(String source) {
        int outerClassIndex = source.lastIndexOf('$');
        if (outerClassIndex == -1) {
            return  source + "JsonConverter";
        } else {
            return source.substring(0, outerClassIndex) + "JsonConverter$" +
                source.substring(outerClassIndex + 1) + "JsonConverter";
        }
    }

    /**
     * Verify that the JSON files support the same message versions as the
     * schemas accessible through the ApiKey class.
     */
    @Test
    public void testMessageVersions() {
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.hasValidVersion()) {
                Message message = null;
                try {
                    message = ApiMessageType.fromApiKey(apiKey.id).newRequest();
                } catch (UnsupportedVersionException e) {
                    fail("No request message spec found for API " + apiKey);
                }
                assertTrue(apiKey.latestVersion() <= message.highestSupportedVersion(),
                        "Request message spec for " + apiKey + " only " + "supports versions up to " +
                                message.highestSupportedVersion());
                try {
                    message = ApiMessageType.fromApiKey(apiKey.id).newResponse();
                } catch (UnsupportedVersionException e) {
                    fail("No response message spec found for API " + apiKey);
                }
                assertTrue(apiKey.latestVersion() <= message.highestSupportedVersion(),
                        "Response message spec for " + apiKey + " only " + "supports versions up to " +
                                message.highestSupportedVersion());
            }
        }
    }

    @Test
    public void testDefaultValues() {
        verifyWriteSucceeds((short) 2,
            new OffsetCommitRequestData().setRetentionTimeMs(123));

        verifyWriteRaisesUve((short) 5, "forgotten",
            new FetchRequestData().setForgottenTopicsData(singletonList(
                new FetchRequestData.ForgottenTopic().setTopic("foo"))));
        verifyWriteSucceeds((short) 5, new FetchRequestData());
        verifyWriteSucceeds((short) 7,
                new FetchRequestData().setForgottenTopicsData(singletonList(
                        new FetchRequestData.ForgottenTopic().setTopic("foo"))));
    }

    @Test
    public void testNonIgnorableFieldWithDefaultNull() {
        // Test non-ignorable string field `groupInstanceId` with default null
        verifyWriteRaisesUve((short) 0, "groupInstanceId", new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(instanceId));
        verifyWriteSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId)
                .setGroupInstanceId(null));
        verifyWriteSucceeds((short) 0, new HeartbeatRequestData()
                .setGroupId("groupId")
                .setGenerationId(15)
                .setMemberId(memberId));
    }

    @Test
    public void testWriteNullForNonNullableFieldRaisesException() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData().setTopics(null);
        for (short version : ApiKeys.CREATE_TOPICS.allVersions()) {
            verifyWriteRaisesNpe(version, createTopics);
        }
        MetadataRequestData metadata = new MetadataRequestData().setTopics(null);
        verifyWriteRaisesNpe((short) 0, metadata);
    }

    @Test
    public void testUnknownTaggedFields() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData();
        verifyWriteSucceeds((short) 6, createTopics);
        RawTaggedField field1000 = new RawTaggedField(1000, new byte[] {0x1, 0x2, 0x3});
        createTopics.unknownTaggedFields().add(field1000);
        verifyWriteRaisesUve((short) 0, "Tagged fields were set", createTopics);
        verifyWriteSucceeds((short) 6, createTopics);
    }

    @Test
    public void testLongTaggedString() {
        char[] chars = new char[1024];
        Arrays.fill(chars, 'a');
        String longString = new String(chars);
        SimpleExampleMessageData message = new SimpleExampleMessageData()
                .setMyString(longString);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short version = 1;
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals(size, buf.position());
    }

    private void verifyWriteRaisesNpe(short version, Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        assertThrows(NullPointerException.class, () -> {
            int size = message.size(cache, version);
            ByteBuffer buf = ByteBuffer.allocate(size);
            ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
            message.write(byteBufferAccessor, cache, version);
        });
    }

    private void verifyWriteRaisesUve(short version,
                                      String problemText,
                                      Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        UnsupportedVersionException e =
            assertThrows(UnsupportedVersionException.class, () -> {
                int size = message.size(cache, version);
                ByteBuffer buf = ByteBuffer.allocate(size);
                ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
                message.write(byteBufferAccessor, cache, version);
            });
        assertTrue(e.getMessage().contains(problemText), "Expected to get an error message about " + problemText +
            ", but got: " + e.getMessage());
    }

    private void verifyWriteSucceeds(short version, Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size * 2);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        message.write(byteBufferAccessor, cache, version);
        assertEquals(size, buf.position(), "Expected the serialized size to be " + size + ", but it was " + buf.position());
    }

    @Test
    public void testCompareWithUnknownTaggedFields() {
        CreateTopicsRequestData createTopics = new CreateTopicsRequestData();
        createTopics.setTimeoutMs(123);
        CreateTopicsRequestData createTopics2 = new CreateTopicsRequestData();
        createTopics2.setTimeoutMs(123);
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
        // Call the accessor, which will create a new empty list.
        createTopics.unknownTaggedFields();
        // Verify that the equalities still hold after the new empty list has been created.
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
        createTopics.unknownTaggedFields().add(new RawTaggedField(0, new byte[] {0}));
        assertNotEquals(createTopics, createTopics2);
        assertNotEquals(createTopics2, createTopics);
        createTopics2.unknownTaggedFields().add(new RawTaggedField(0, new byte[] {0}));
        assertEquals(createTopics, createTopics2);
        assertEquals(createTopics2, createTopics);
    }
}
