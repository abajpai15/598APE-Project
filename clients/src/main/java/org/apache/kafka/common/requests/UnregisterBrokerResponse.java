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

import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.EnumMap;
import java.util.Map;

public class UnregisterBrokerResponse extends AbstractResponse {
    private final UnregisterBrokerResponseData data;

    public UnregisterBrokerResponse(UnregisterBrokerResponseData data) {
        super(ApiKeys.UNREGISTER_BROKER);
        this.data = data;
    }

    @Override
    public UnregisterBrokerResponseData data() {
        return data;
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
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new EnumMap<>(Errors.class);
        if (data.errorCode() != 0) {
            errorCounts.put(Errors.forCode(data.errorCode()), 1);
        }
        return errorCounts;
    }

    public static UnregisterBrokerResponse parse(Readable readable, short version) {
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData(readable, version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }
}
