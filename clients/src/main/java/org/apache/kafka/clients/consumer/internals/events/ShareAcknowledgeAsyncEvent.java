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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.NodeAcknowledgements;
import org.apache.kafka.common.TopicIdPartition;

import java.util.Map;

public class ShareAcknowledgeAsyncEvent extends ApplicationEvent {

    private final Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap;
    private final long deadlineMs;

    public ShareAcknowledgeAsyncEvent(final Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap,
                                      final long deadlineMs) {
        super(Type.SHARE_ACKNOWLEDGE_ASYNC);
        this.acknowledgementsMap = acknowledgementsMap;
        this.deadlineMs = deadlineMs;
    }

    public Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap() {
        return acknowledgementsMap;
    }

    public long deadlineMs() {
        return deadlineMs;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", acknowledgementsMap=" + acknowledgementsMap + ", deadlineMs=" + deadlineMs;
    }
}
