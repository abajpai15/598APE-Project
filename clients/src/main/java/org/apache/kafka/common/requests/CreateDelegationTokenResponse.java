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

import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.Map;

public class CreateDelegationTokenResponse extends AbstractResponse {

    private final CreateDelegationTokenResponseData data;

    public CreateDelegationTokenResponse(CreateDelegationTokenResponseData data) {
        super(ApiKeys.CREATE_DELEGATION_TOKEN);
        this.data = data;
    }

    public static CreateDelegationTokenResponse parse(Readable readable, short version) {
        return new CreateDelegationTokenResponse(
            new CreateDelegationTokenResponseData(readable, version));
    }

    public static CreateDelegationTokenResponse prepareResponse(int version,
            int throttleTimeMs,
            Errors error,
            KafkaPrincipal owner,
            KafkaPrincipal tokenRequester,
            long issueTimestamp,
            long expiryTimestamp,
            long maxTimestamp,
            String tokenId,
            ByteBuffer hmac) {
        CreateDelegationTokenResponseData data = new CreateDelegationTokenResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setPrincipalType(owner.getPrincipalType())
                .setPrincipalName(owner.getName())
                .setIssueTimestampMs(issueTimestamp)
                .setExpiryTimestampMs(expiryTimestamp)
                .setMaxTimestampMs(maxTimestamp)
                .setTokenId(tokenId)
                .setHmac(hmac.array());
        if (version > 2) {
            data.setTokenRequesterPrincipalType(tokenRequester.getPrincipalType())
                .setTokenRequesterPrincipalName(tokenRequester.getName());
        }
        return new CreateDelegationTokenResponse(data);
    }

    public static CreateDelegationTokenResponse prepareResponse(int version, int throttleTimeMs, Errors error,
                                                                KafkaPrincipal owner, KafkaPrincipal requester) {
        return prepareResponse(version, throttleTimeMs, error, owner, requester, -1, -1, -1, "", ByteBuffer.wrap(new byte[] {}));
    }

    @Override
    public CreateDelegationTokenResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
