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

import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.List;
import java.util.stream.Collectors;

public class ShareGroupDescribeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareGroupDescribeRequest> {

        private final ShareGroupDescribeRequestData data;

        public Builder(ShareGroupDescribeRequestData data) {
            super(ApiKeys.SHARE_GROUP_DESCRIBE);
            this.data = data;
        }

        @Override
        public ShareGroupDescribeRequest build(short version) {
            return new ShareGroupDescribeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareGroupDescribeRequestData data;

    public ShareGroupDescribeRequest(ShareGroupDescribeRequestData data, short version) {
        super(ApiKeys.SHARE_GROUP_DESCRIBE, version);
        this.data = data;
    }

    @Override
    public ShareGroupDescribeResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ShareGroupDescribeResponseData data = new ShareGroupDescribeResponseData()
                .setThrottleTimeMs(throttleTimeMs);
        // Set error for each group
        short errorCode = Errors.forException(e).code();
        this.data.groupIds().forEach(
                groupId -> data.groups().add(
                        new ShareGroupDescribeResponseData.DescribedGroup()
                                .setGroupId(groupId)
                                .setErrorCode(errorCode)
                )
        );
        return new ShareGroupDescribeResponse(data);
    }

    @Override
    public ShareGroupDescribeRequestData data() {
        return data;
    }

    public static ShareGroupDescribeRequest parse(Readable readable, short version) {
        return new ShareGroupDescribeRequest(
                new ShareGroupDescribeRequestData(readable, version),
                version
        );
    }

    public static List<ShareGroupDescribeResponseData.DescribedGroup> getErrorDescribedGroupList(
            List<String> groupIds,
            Errors error
    ) {
        return groupIds.stream()
                .map(groupId -> new ShareGroupDescribeResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
                ).collect(Collectors.toList());
    }
}
