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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ConsumerGroupHeartbeatRequest extends AbstractRequest {

    /**
     * A member epoch of <code>-1</code> means that the member wants to leave the group.
     */
    public static final int LEAVE_GROUP_MEMBER_EPOCH = -1;
    public static final int LEAVE_GROUP_STATIC_MEMBER_EPOCH = -2;

    /**
     * A member epoch of <code>0</code> means that the member wants to join the group.
     */
    public static final int JOIN_GROUP_MEMBER_EPOCH = 0;

    /**
     * The version from which consumers are required to generate their own member id.
     *
     * <p>Starting from this version, member id must be generated by the consumer instance
     * instead of being provided by the server.</p>
     */
    public static final int CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION = 1;

    public static final String REGEX_RESOLUTION_NOT_SUPPORTED_MSG = "The cluster does not support " +
        "regular expressions resolution on ConsumerGroupHeartbeat API version 0. It must be upgraded to use " +
        "ConsumerGroupHeartbeat API version >= 1 to allow to subscribe to a SubscriptionPattern.";

    public static class Builder extends AbstractRequest.Builder<ConsumerGroupHeartbeatRequest> {
        private final ConsumerGroupHeartbeatRequestData data;

        public Builder(ConsumerGroupHeartbeatRequestData data) {
            this(data, false);
        }

        public Builder(ConsumerGroupHeartbeatRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.CONSUMER_GROUP_HEARTBEAT, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ConsumerGroupHeartbeatRequest build(short version) {
            if (version == 0 && data.subscribedTopicRegex() != null) {
                throw new UnsupportedVersionException(REGEX_RESOLUTION_NOT_SUPPORTED_MSG);
            }
            return new ConsumerGroupHeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ConsumerGroupHeartbeatRequestData data;

    public ConsumerGroupHeartbeatRequest(ConsumerGroupHeartbeatRequestData data, short version) {
        super(ApiKeys.CONSUMER_GROUP_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ConsumerGroupHeartbeatResponse(
            new ConsumerGroupHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public ConsumerGroupHeartbeatRequestData data() {
        return data;
    }

    public static ConsumerGroupHeartbeatRequest parse(Readable readable, short version) {
        return new ConsumerGroupHeartbeatRequest(new ConsumerGroupHeartbeatRequestData(
            readable, version), version);
    }
}
