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

import org.apache.kafka.common.message.BrokerRegistrationResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.EnumMap;
import java.util.Map;

public class BrokerRegistrationResponse extends AbstractResponse {
    private final BrokerRegistrationResponseData data;

    public BrokerRegistrationResponse(BrokerRegistrationResponseData data) {
        super(ApiKeys.BROKER_REGISTRATION);
        this.data = data;
    }

    @Override
    public BrokerRegistrationResponseData data() {
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
        errorCounts.put(Errors.forCode(data.errorCode()), 1);
        return errorCounts;
    }

    public static BrokerRegistrationResponse parse(Readable readable, short version) {
        return new BrokerRegistrationResponse(new BrokerRegistrationResponseData(readable, version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
