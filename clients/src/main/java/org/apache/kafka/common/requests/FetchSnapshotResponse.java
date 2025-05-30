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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

public final class FetchSnapshotResponse extends AbstractResponse {
    private final FetchSnapshotResponseData data;

    public FetchSnapshotResponse(FetchSnapshotResponseData data) {
        super(ApiKeys.FETCH_SNAPSHOT);
        this.data = data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new EnumMap<>(Errors.class);

        Errors topLevelError = Errors.forCode(data.errorCode());
        if (topLevelError != Errors.NONE) {
            errors.put(topLevelError, 1);
        }

        for (FetchSnapshotResponseData.TopicSnapshot topicResponse : data.topics()) {
            for (FetchSnapshotResponseData.PartitionSnapshot partitionResponse : topicResponse.partitions()) {
                errors.compute(Errors.forCode(partitionResponse.errorCode()),
                    (error, count) -> count == null ? 1 : count + 1);
            }
        }

        return errors;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public FetchSnapshotResponseData data() {
        return data;
    }

    /**
     * Creates a FetchSnapshotResponseData with a top level error.
     *
     * @param error the top level error
     * @return the created fetch snapshot response data
     */
    public static FetchSnapshotResponseData withTopLevelError(Errors error) {
        return new FetchSnapshotResponseData().setErrorCode(error.code());
    }

    /**
     * Finds the PartitionSnapshot for a given topic partition.
     *
     * @param data the fetch snapshot response data
     * @param topicPartition the topic partition to find
     * @return the response partition snapshot if found, otherwise an empty Optional
     */
    public static Optional<FetchSnapshotResponseData.PartitionSnapshot> forTopicPartition(
        FetchSnapshotResponseData data,
        TopicPartition topicPartition
    ) {
        return data
            .topics()
            .stream()
            .filter(topic -> topic.name().equals(topicPartition.topic()))
            .flatMap(topic -> topic.partitions().stream())
            .filter(partition -> partition.index() == topicPartition.partition())
            .findAny();
    }

    public static FetchSnapshotResponse parse(Readable readable, short version) {
        return new FetchSnapshotResponse(new FetchSnapshotResponseData(readable, version));
    }
}
