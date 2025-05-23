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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Possible error codes:
 * - {@link Errors#UNSUPPORTED_VERSION}
 * - {@link Errors#INVALID_REQUEST}
 */
public class ApiVersionsResponse extends AbstractResponse {

    public static final long UNKNOWN_FINALIZED_FEATURES_EPOCH = -1L;

    private final ApiVersionsResponseData data;

    public static class Builder {
        private Errors error = Errors.NONE;
        private int throttleTimeMs = 0;
        private ApiVersionCollection apiVersions = null;
        private Features<SupportedVersionRange> supportedFeatures = null;
        private Map<String, Short> finalizedFeatures = null;
        private long finalizedFeaturesEpoch = 0;
        private boolean zkMigrationEnabled = false;
        private boolean alterFeatureLevel0 = false;

        public Builder setError(Errors error) {
            this.error = error;
            return this;
        }

        public Builder setThrottleTimeMs(int throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs;
            return this;
        }

        public Builder setApiVersions(ApiVersionCollection apiVersions) {
            this.apiVersions = apiVersions;
            return this;
        }

        public Builder setSupportedFeatures(Features<SupportedVersionRange> supportedFeatures) {
            this.supportedFeatures = supportedFeatures;
            return this;
        }

        public Builder setFinalizedFeatures(Map<String, Short> finalizedFeatures) {
            this.finalizedFeatures = finalizedFeatures;
            return this;
        }

        public Builder setFinalizedFeaturesEpoch(long finalizedFeaturesEpoch) {
            this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
            return this;
        }

        public Builder setZkMigrationEnabled(boolean zkMigrationEnabled) {
            this.zkMigrationEnabled = zkMigrationEnabled;
            return this;
        }

        public Builder setAlterFeatureLevel0(boolean alterFeatureLevel0) {
            this.alterFeatureLevel0 = alterFeatureLevel0;
            return this;
        }

        public ApiVersionsResponse build() {
            final ApiVersionsResponseData data = new ApiVersionsResponseData();
            data.setErrorCode(error.code());
            data.setApiKeys(Objects.requireNonNull(apiVersions));
            data.setThrottleTimeMs(throttleTimeMs);
            data.setSupportedFeatures(
                maybeFilterSupportedFeatureKeys(Objects.requireNonNull(supportedFeatures), alterFeatureLevel0));
            data.setFinalizedFeatures(
                createFinalizedFeatureKeys(Objects.requireNonNull(finalizedFeatures)));
            data.setFinalizedFeaturesEpoch(finalizedFeaturesEpoch);
            data.setZkMigrationReady(zkMigrationEnabled);
            return new ApiVersionsResponse(data);
        }
    }

    public ApiVersionsResponse(ApiVersionsResponseData data) {
        super(ApiKeys.API_VERSIONS);
        this.data = data;
    }

    @Override
    public ApiVersionsResponseData data() {
        return data;
    }

    public ApiVersion apiVersion(short apiKey) {
        return data.apiKeys().find(apiKey);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(this.data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public boolean zkMigrationReady() {
        return data.zkMigrationReady();
    }

    public static ApiVersionsResponse parse(Readable readable, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it which means that the version received by this
        // method is not necessarily the real one. It may be version 0 as well.
        Readable readableCopy = readable.slice();
        try {
            return new ApiVersionsResponse(new ApiVersionsResponseData(readable, version));
        } catch (RuntimeException e) {
            if (version != 0)
                return new ApiVersionsResponse(new ApiVersionsResponseData(readableCopy, (short) 0));
            else
                throw e;
        }
    }

    public static ApiVersionCollection controllerApiVersions(
        NodeApiVersions controllerApiVersions,
        ListenerType listenerType,
        boolean enableUnstableLastVersion,
        boolean clientTelemetryEnabled
    ) {
        return intersectForwardableApis(
            listenerType,
            controllerApiVersions.allSupportedApiVersions(),
            enableUnstableLastVersion,
            clientTelemetryEnabled);
    }

    public static ApiVersionCollection brokerApiVersions(
        ListenerType listenerType,
        boolean enableUnstableLastVersion,
        boolean clientTelemetryEnabled
    ) {
        return filterApis(
            listenerType,
            enableUnstableLastVersion,
            clientTelemetryEnabled);
    }

    public static ApiVersionCollection filterApis(
        ApiMessageType.ListenerType listenerType,
        boolean enableUnstableLastVersion,
        boolean clientTelemetryEnabled
    ) {
        ApiVersionCollection apiKeys = new ApiVersionCollection();
        for (ApiKeys apiKey : ApiKeys.apisForListener(listenerType)) {
            // Skip telemetry APIs if client telemetry is disabled.
            if ((apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY) && !clientTelemetryEnabled)
                continue;
            apiKey.toApiVersionForApiResponse(enableUnstableLastVersion, listenerType).ifPresent(apiKeys::add);
        }
        return apiKeys;
    }

    public static ApiVersionCollection collectApis(
        ApiMessageType.ListenerType listenerType,
        Set<ApiKeys> apiKeys,
        boolean enableUnstableLastVersion
    ) {
        ApiVersionCollection res = new ApiVersionCollection();
        for (ApiKeys apiKey : apiKeys) {
            apiKey.toApiVersionForApiResponse(enableUnstableLastVersion, listenerType).ifPresent(res::add);
        }
        return res;
    }

    /**
     * Find the common range of supported API versions between the locally
     * known range and that of another set.
     *
     * @param listenerType the listener type which constrains the set of exposed APIs
     * @param activeControllerApiVersions controller ApiVersions
     * @param enableUnstableLastVersion whether unstable versions should be advertised or not
     * @param clientTelemetryEnabled whether client telemetry is enabled or not
     * @return commonly agreed ApiVersion collection
     */
    public static ApiVersionCollection intersectForwardableApis(
        final ApiMessageType.ListenerType listenerType,
        final Map<ApiKeys, ApiVersion> activeControllerApiVersions,
        boolean enableUnstableLastVersion,
        boolean clientTelemetryEnabled
    ) {
        ApiVersionCollection apiKeys = new ApiVersionCollection();
        for (ApiKeys apiKey : ApiKeys.apisForListener(listenerType)) {
            final Optional<ApiVersion> brokerApiVersion = apiKey.toApiVersionForApiResponse(enableUnstableLastVersion, listenerType);
            if (brokerApiVersion.isEmpty()) {
                // Broker does not support this API key.
                continue;
            }

            // Skip telemetry APIs if client telemetry is disabled.
            if ((apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY) && !clientTelemetryEnabled)
                continue;

            final ApiVersion finalApiVersion;
            if (!apiKey.forwardable) {
                finalApiVersion = brokerApiVersion.get();
            } else {
                Optional<ApiVersion> intersectVersion = intersect(
                    brokerApiVersion.get(),
                    activeControllerApiVersions.getOrDefault(apiKey, null)
                );
                if (intersectVersion.isPresent()) {
                    finalApiVersion = intersectVersion.get();
                } else {
                    // Controller doesn't support this API key, or there is no intersection.
                    continue;
                }
            }

            apiKeys.add(finalApiVersion.duplicate());
        }
        return apiKeys;
    }

    private static SupportedFeatureKeyCollection maybeFilterSupportedFeatureKeys(
        Features<SupportedVersionRange> latestSupportedFeatures,
        boolean alterV0
    ) {
        SupportedFeatureKeyCollection converted = new SupportedFeatureKeyCollection();
        for (Map.Entry<String, SupportedVersionRange> feature : latestSupportedFeatures.features().entrySet()) {
            final SupportedVersionRange versionRange = feature.getValue();
            if (alterV0 && versionRange.min() == 0) {
                // Some older clients will have deserialization problems if a feature's
                // minimum supported level is 0. Therefore, when preparing ApiVersionResponse
                // at versions less than 4, we must omit these features. See KAFKA-17492.
            } else {
                final SupportedFeatureKey key = new SupportedFeatureKey();
                key.setName(feature.getKey());
                key.setMinVersion(versionRange.min());
                key.setMaxVersion(versionRange.max());
                converted.add(key);
            }
        }

        return converted;
    }

    private static FinalizedFeatureKeyCollection createFinalizedFeatureKeys(
        Map<String, Short> finalizedFeatures) {
        FinalizedFeatureKeyCollection converted = new FinalizedFeatureKeyCollection();
        for (Map.Entry<String, Short> feature : finalizedFeatures.entrySet()) {
            final FinalizedFeatureKey key = new FinalizedFeatureKey();
            final short versionLevel = feature.getValue();
            if (versionLevel != 0) {
                key.setName(feature.getKey());
                key.setMinVersionLevel(versionLevel);
                key.setMaxVersionLevel(versionLevel);
                converted.add(key);
            }
        }

        return converted;
    }

    public static Optional<ApiVersion> intersect(ApiVersion thisVersion,
                                                 ApiVersion other) {
        if (thisVersion == null || other == null) return Optional.empty();
        if (thisVersion.apiKey() != other.apiKey())
            throw new IllegalArgumentException("thisVersion.apiKey: " + thisVersion.apiKey()
                + " must be equal to other.apiKey: " + other.apiKey());
        short minVersion = (short) Math.max(thisVersion.minVersion(), other.minVersion());
        short maxVersion = (short) Math.min(thisVersion.maxVersion(), other.maxVersion());
        return minVersion > maxVersion
                ? Optional.empty()
                : Optional.of(new ApiVersion()
                    .setApiKey(thisVersion.apiKey())
                    .setMinVersion(minVersion)
                    .setMaxVersion(maxVersion));
    }

    public static ApiVersion toApiVersion(ApiKeys apiKey) {
        return new ApiVersion()
            .setApiKey(apiKey.id)
            .setMinVersion(apiKey.oldestVersion())
            .setMaxVersion(apiKey.latestVersion());
    }
}
