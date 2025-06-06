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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaveGroupResponseTest {

    private final int throttleTimeMs = 10;

    private List<MemberResponse> memberResponses;

    @BeforeEach
    public void setUp() {
        memberResponses = Arrays.asList(new MemberResponse()
                                            .setMemberId("member_1")
                                            .setGroupInstanceId("instance_1")
                                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                                        new MemberResponse()
                                            .setMemberId("member_2")
                                            .setGroupInstanceId("instance_2")
                                            .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
        );
    }

    @Test
    public void testConstructorWithMemberResponses() {
        Map<Errors, Integer> expectedErrorCounts = new EnumMap<>(Errors.class);
        expectedErrorCounts.put(Errors.NONE, 1); // top level
        expectedErrorCounts.put(Errors.UNKNOWN_MEMBER_ID, 1);
        expectedErrorCounts.put(Errors.FENCED_INSTANCE_ID, 1);

        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(memberResponses,
                                                                           Errors.NONE,
                                                                           throttleTimeMs,
                                                                           version);

            if (version >= 3) {
                assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts());
                assertEquals(memberResponses, leaveGroupResponse.memberResponses());
            } else {
                assertEquals(Collections.singletonMap(Errors.UNKNOWN_MEMBER_ID, 1),
                             leaveGroupResponse.errorCounts());
                assertEquals(Collections.emptyList(), leaveGroupResponse.memberResponses());
            }

            if (version >= 1) {
                assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs());
            }

            assertEquals(Errors.UNKNOWN_MEMBER_ID, leaveGroupResponse.error());
        }
    }

    @Test
    public void testShouldThrottle() {
        LeaveGroupResponse response = new LeaveGroupResponse(new LeaveGroupResponseData());
        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            if (version >= 2) {
                assertTrue(response.shouldClientThrottle(version));
            } else {
                assertFalse(response.shouldClientThrottle(version));
            }
        }
    }

    @Test
    public void testEqualityWithSerialization() {
        LeaveGroupResponseData responseData = new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(throttleTimeMs);
        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            LeaveGroupResponse primaryResponse = LeaveGroupResponse.parse(
                MessageUtil.toByteBufferAccessor(responseData, version), version);
            LeaveGroupResponse secondaryResponse = LeaveGroupResponse.parse(
                MessageUtil.toByteBufferAccessor(responseData, version), version);

            assertEquals(primaryResponse, primaryResponse);
            assertEquals(primaryResponse, secondaryResponse);
            assertEquals(primaryResponse.hashCode(), secondaryResponse.hashCode());
        }
    }

    @Test
    public void testParse() {
        Map<Errors, Integer> expectedErrorCounts = Collections.singletonMap(Errors.NOT_COORDINATOR, 1);

        LeaveGroupResponseData data = new LeaveGroupResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code())
            .setThrottleTimeMs(throttleTimeMs);

        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            Readable buffer = MessageUtil.toByteBufferAccessor(data, version);
            LeaveGroupResponse leaveGroupResponse = LeaveGroupResponse.parse(buffer, version);
            assertEquals(expectedErrorCounts, leaveGroupResponse.errorCounts());

            if (version >= 1) {
                assertEquals(throttleTimeMs, leaveGroupResponse.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, leaveGroupResponse.throttleTimeMs());
            }

            assertEquals(Errors.NOT_COORDINATOR, leaveGroupResponse.error());
        }
    }

    @Test
    public void testEqualityWithMemberResponses() {
        for (short version : ApiKeys.LEAVE_GROUP.allVersions()) {
            List<MemberResponse> localResponses = version > 2 ? memberResponses : memberResponses.subList(0, 1);
            LeaveGroupResponse primaryResponse = new LeaveGroupResponse(localResponses,
                                                                        Errors.NONE,
                                                                        throttleTimeMs,
                                                                        version);

            // The order of members should not alter result data.
            Collections.reverse(localResponses);
            LeaveGroupResponse reversedResponse = new LeaveGroupResponse(localResponses,
                                                                         Errors.NONE,
                                                                         throttleTimeMs,
                                                                         version);

            assertEquals(primaryResponse, primaryResponse);
            assertEquals(primaryResponse, reversedResponse);
            assertEquals(primaryResponse.hashCode(), reversedResponse.hashCode());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
    public void testNoErrorNoMembersResponses(short version) {
        LeaveGroupResponseData data = new LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMembers(Collections.emptyList());

        if (version < 3) {
            assertThrows(UnsupportedVersionException.class,
                () -> new LeaveGroupResponse(data, version));
        } else {
            LeaveGroupResponse response = new LeaveGroupResponse(data, version);
            assertEquals(Errors.NONE, response.topLevelError());
            assertEquals(Collections.emptyList(), response.memberResponses());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
    public void testNoErrorMultipleMembersResponses(short version) {
        LeaveGroupResponseData data = new LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMembers(memberResponses);

        if (version < 3) {
            assertThrows(UnsupportedVersionException.class,
                () -> new LeaveGroupResponse(data, version));
        } else {
            LeaveGroupResponse response = new LeaveGroupResponse(data, version);
            assertEquals(Errors.NONE, response.topLevelError());
            assertEquals(memberResponses, response.memberResponses());
        }
    }
    
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
    public void testErrorResponses(short version) {
        LeaveGroupResponseData dataNoMembers = new LeaveGroupResponseData()
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setMembers(Collections.emptyList());

        LeaveGroupResponse responseNoMembers = new LeaveGroupResponse(dataNoMembers, version);
        assertEquals(Errors.GROUP_ID_NOT_FOUND, responseNoMembers.topLevelError());
        
        LeaveGroupResponseData dataMembers = new LeaveGroupResponseData()
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setMembers(memberResponses);
        
        LeaveGroupResponse responseMembers = new LeaveGroupResponse(dataMembers, version);
        assertEquals(Errors.GROUP_ID_NOT_FOUND, responseMembers.topLevelError());
    }

}
