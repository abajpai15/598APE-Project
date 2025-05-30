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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


public class FindCoordinatorResponse extends AbstractResponse {

    /**
     * Possible error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE (15)
     * GROUP_AUTHORIZATION_FAILED (30)
     * INVALID_REQUEST (42)
     * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
     */

    private final FindCoordinatorResponseData data;

    public FindCoordinatorResponse(FindCoordinatorResponseData data) {
        super(ApiKeys.FIND_COORDINATOR);
        this.data = data;
    }

    public Optional<Coordinator> coordinatorByKey(String key) {
        Objects.requireNonNull(key);
        if (this.data.coordinators().isEmpty()) {
            // version <= 3
            return Optional.of(new Coordinator()
                    .setErrorCode(data.errorCode())
                    .setErrorMessage(data.errorMessage())
                    .setHost(data.host())
                    .setPort(data.port())
                    .setNodeId(data.nodeId())
                    .setKey(key));
        }
        // version >= 4
        return data.coordinators().stream().filter(c -> c.key().equals(key)).findFirst();
    }

    @Override
    public FindCoordinatorResponseData data() {
        return data;
    }

    public Node node() {
        return new Node(data.nodeId(), data.host(), data.port());
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (!data.coordinators().isEmpty()) {
            Map<Errors, Integer> errorCounts = new EnumMap<>(Errors.class);
            for (Coordinator coordinator : data.coordinators()) {
                updateErrorCounts(errorCounts, Errors.forCode(coordinator.errorCode()));
            }
            return errorCounts;
        } else {
            return errorCounts(error());
        }
    }

    public static FindCoordinatorResponse parse(Readable readable, short version) {
        return new FindCoordinatorResponse(new FindCoordinatorResponseData(readable, version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public List<FindCoordinatorResponseData.Coordinator> coordinators() {
        if (!data.coordinators().isEmpty())
            return data.coordinators();
        else {
            FindCoordinatorResponseData.Coordinator coordinator = new Coordinator()
                    .setErrorCode(data.errorCode())
                    .setErrorMessage(data.errorMessage())
                    .setKey(null)
                    .setNodeId(data.nodeId())
                    .setHost(data.host())
                    .setPort(data.port());
            return Collections.singletonList(coordinator);
        }
    }

    public static FindCoordinatorResponse prepareOldResponse(Errors error, Node node) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.setErrorCode(error.code())
            .setErrorMessage(error.message())
            .setNodeId(node.id())
            .setHost(node.host())
            .setPort(node.port());
        return new FindCoordinatorResponse(data);
    }

    public static FindCoordinatorResponse prepareResponse(Errors error, String key, Node node) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.setCoordinators(Collections.singletonList(
            prepareCoordinatorResponse(error, key, node)
        ));
        return new FindCoordinatorResponse(data);
    }

    public static FindCoordinatorResponseData.Coordinator prepareCoordinatorResponse(
        Errors error,
        String key,
        Node node
    ) {
        return new FindCoordinatorResponseData.Coordinator()
            .setErrorCode(error.code())
            .setErrorMessage(error.message())
            .setKey(key)
            .setHost(node.host())
            .setPort(node.port())
            .setNodeId(node.id());
    }

    public static FindCoordinatorResponse prepareErrorResponse(Errors error, List<String> keys) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        List<FindCoordinatorResponseData.Coordinator> coordinators = new ArrayList<>(keys.size());
        for (String key : keys) {
            FindCoordinatorResponseData.Coordinator coordinator = new FindCoordinatorResponseData.Coordinator()
                .setErrorCode(error.code())
                .setErrorMessage(error.message())
                .setKey(key)
                .setHost(Node.noNode().host())
                .setPort(Node.noNode().port())
                .setNodeId(Node.noNode().id());
            coordinators.add(coordinator);
        }
        data.setCoordinators(coordinators);
        return new FindCoordinatorResponse(data);
    }

}
