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

import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.EnumMap;
import java.util.Map;

/**
 * Possible error codes.
 *
 * Top level errors:
 * - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * - {@link Errors#BROKER_NOT_AVAILABLE}
 *
 * Partition level errors:
 * - {@link Errors#FENCED_LEADER_EPOCH}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#INCONSISTENT_VOTER_SET}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 */
public class VoteResponse extends AbstractResponse {
    private final VoteResponseData data;

    public VoteResponse(VoteResponseData data) {
        super(ApiKeys.VOTE);
        this.data = data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new EnumMap<>(Errors.class);

        errors.put(Errors.forCode(data.errorCode()), 1);

        for (VoteResponseData.TopicData topicResponse : data.topics()) {
            for (VoteResponseData.PartitionData partitionResponse : topicResponse.partitions()) {
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode()));
            }
        }
        return errors;
    }

    @Override
    public VoteResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // Not supported by the response schema
    }

    public static VoteResponse parse(Readable readable, short version) {
        return new VoteResponse(new VoteResponseData(readable, version));
    }
}
