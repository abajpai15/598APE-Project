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

import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.List;
import java.util.Map;

public class CreateAclsResponse extends AbstractResponse {
    private final CreateAclsResponseData data;

    public CreateAclsResponse(CreateAclsResponseData data) {
        super(ApiKeys.CREATE_ACLS);
        this.data = data;
    }

    @Override
    public CreateAclsResponseData data() {
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

    public List<CreateAclsResponseData.AclCreationResult> results() {
        return data.results();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(results().stream().map(r -> Errors.forCode(r.errorCode())));
    }

    public static CreateAclsResponse parse(Readable readable, short version) {
        return new CreateAclsResponse(new CreateAclsResponseData(readable, version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
