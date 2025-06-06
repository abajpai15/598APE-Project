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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ConfigSynonym;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SUBTRACT;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.apache.kafka.server.config.ConfigSynonym.HOURS_TO_MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ConfigurationControlManagerTest {

    static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux").
            define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                ConfigDef.Type.INT, "1", ConfigDef.Importance.HIGH, "min.isr"));

        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc").
            define("def", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "def").
            define("ghi", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "ghi").
            define("quuux", ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "quux").
            define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, ""));
    }

    public static final Map<String, List<ConfigSynonym>> SYNONYMS = new HashMap<>();

    static {
        SYNONYMS.put("abc", List.of(new ConfigSynonym("foo.bar")));
        SYNONYMS.put("def", List.of(new ConfigSynonym("baz")));
        SYNONYMS.put("quuux", List.of(new ConfigSynonym("quux", HOURS_TO_MILLISECONDS)));
        SYNONYMS.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, List.of(new ConfigSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)));
    }

    static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS, SYNONYMS);

    static final ConfigResource BROKER0 = new ConfigResource(BROKER, "0");
    static final ConfigResource MYTOPIC = new ConfigResource(TOPIC, "mytopic");

    static class TestExistenceChecker implements Consumer<ConfigResource> {
        static final TestExistenceChecker INSTANCE = new TestExistenceChecker();

        @Override
        public void accept(ConfigResource resource) {
            if (!resource.name().startsWith("Existing")) {
                throw new UnknownTopicOrPartitionException("Unknown resource.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <A, B> Map<A, B> toMap(Entry... entries) {
        Map<A, B> map = new LinkedHashMap<>();
        for (Entry<A, B> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    static <A, B> Entry<A, B> entry(A a, B b) {
        return new SimpleImmutableEntry<>(a, b);
    }

    @Test
    public void testReplay() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        assertEquals(Map.of(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue("1,2"));
        assertEquals(Map.of("foo.bar", "1,2"),
            manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue(null));
        assertEquals(Map.of(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("abc").setValue("x,y,z"));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("blah"));
        assertEquals(toMap(entry("abc", "x,y,z"), entry("def", "blah")),
            manager.getConfigs(MYTOPIC));
        assertEquals("x,y,z", manager.getTopicConfig(MYTOPIC.name(), "abc").value());
    }

    @Test
    public void testIncrementalAlterConfigs() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            build();

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("baz", entry(SUBTRACT, "abc")),
                entry("quux", entry(SET, "abc")))),
                entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123"))))),
                true);

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(BROKER0, new ApiError(Errors.INVALID_CONFIG,
                            "Can't SUBTRACT to key baz because its type is not LIST.")),
                    entry(MYTOPIC, ApiError.NONE))), result);

        RecordTestUtils.replayAll(manager, result.records());

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue(null), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("abc", entry(DELETE, "xyz"))))),
                true));
    }

    @Test
    public void testIncrementalAlterConfig() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            build();
        Map<String, Entry<AlterConfigOp.OpType, String>> keyToOps = toMap(entry("abc", entry(APPEND, "123")));

        ControllerResult<ApiError> result = manager.
            incrementalAlterConfig(MYTOPIC, keyToOps, true);

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123"), CONFIG_RECORD.highestSupportedVersion())),
            ApiError.NONE), result);

        RecordTestUtils.replayAll(manager, result.records());

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                    new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                        setName("abc").setValue(null), CONFIG_RECORD.highestSupportedVersion())),
                ApiError.NONE),
            manager.incrementalAlterConfig(MYTOPIC, toMap(entry("abc", entry(DELETE, "xyz"))), true));

        // The configuration value exceeding the maximum size is not allowed to be added.
        String largeValue = new String(new char[Short.MAX_VALUE - APPEND.id() - 1]);
        Map<String, Entry<AlterConfigOp.OpType, String>> largeValueOfOps = toMap(entry("abc", entry(APPEND, largeValue)));

        ControllerResult<ApiError> invalidConfigValueResult = manager.incrementalAlterConfig(MYTOPIC, largeValueOfOps, true);
        assertEquals(Errors.INVALID_CONFIG, invalidConfigValueResult.response().error());
        assertEquals("The configuration value cannot be added because it exceeds the maximum value size of " + Short.MAX_VALUE + " bytes.",
                invalidConfigValueResult.response().message());
    }

    @Test
    public void testIncrementalAlterMultipleConfigValues() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            build();

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123,456,789"))))), true);

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123,456,789"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))), result);

        RecordTestUtils.replayAll(manager, result.records());

        // It's ok for the appended value to be already present
        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123,456"))))), true);
        assertEquals(
            ControllerResult.atomicOf(List.of(), toMap(entry(MYTOPIC, ApiError.NONE))),
            result
        );
        RecordTestUtils.replayAll(manager, result.records());

        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(SUBTRACT, "123,456"))))), true);
        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("789"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))),
                result);
        RecordTestUtils.replayAll(manager, result.records());

        // It's ok for the deleted value not to be present
        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(SUBTRACT, "123456"))))), true);
        assertEquals(
            ControllerResult.atomicOf(List.of(), toMap(entry(MYTOPIC, ApiError.NONE))),
            result
        );
        RecordTestUtils.replayAll(manager, result.records());

        assertEquals("789", manager.getConfigs(MYTOPIC).get("abc"));
    }

    @Test
    public void testIncrementalAlterConfigsWithoutExistence() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            setExistenceChecker(TestExistenceChecker.INSTANCE).
            build();
        ConfigResource existingTopic = new ConfigResource(TOPIC, "ExistingTopic");

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("quux", entry(SET, "1")))),
                entry(existingTopic, toMap(entry("def", entry(SET, "newVal"))))),
                false);

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("ExistingTopic").
                    setName("def").setValue("newVal"), CONFIG_RECORD.highestSupportedVersion())),
            toMap(entry(BROKER0, new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    "Unknown resource.")),
                entry(existingTopic, ApiError.NONE))), result);
    }

    private static class MockAlterConfigsPolicy implements AlterConfigPolicy {
        private final List<RequestMetadata> expecteds;
        private final AtomicLong index = new AtomicLong(0);

        MockAlterConfigsPolicy(List<RequestMetadata> expecteds) {
            this.expecteds = expecteds;
        }

        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            long curIndex = index.getAndIncrement();
            if (curIndex >= expecteds.size()) {
                throw new PolicyViolationException("Unexpected config alteration: index " +
                    "out of range at " + curIndex);
            }
            RequestMetadata expected = expecteds.get((int) curIndex);
            if (!expected.equals(actual)) {
                throw new PolicyViolationException("Expected: " + expected +
                    ". Got: " + actual);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // nothing to do
        }
    }

    @Test
    public void testIncrementalAlterConfigsWithPolicy() {
        MockAlterConfigsPolicy policy = new MockAlterConfigsPolicy(List.of(
            new RequestMetadata(MYTOPIC, Map.of()),
            new RequestMetadata(BROKER0, toMap(
                entry("foo.bar", "123"),
                entry("quux", "456"),
                entry("broker.config.to.remove", null)))));
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            setAlterConfigPolicy(Optional.of(policy)).
            build();
        // Existing configs should not be passed to the policy
        manager.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                setName("broker.config").setValue("123"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName(MYTOPIC.name()).
                setName("topic.config").setValue("123"));
        manager.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                setName("broker.config.to.remove").setValue("123"));
        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                    setName("foo.bar").setValue("123"), CONFIG_RECORD.highestSupportedVersion()), new ApiMessageAndVersion(
                                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                                        setName("quux").setValue("456"), CONFIG_RECORD.highestSupportedVersion()), new ApiMessageAndVersion(
                                            new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                                                    setName("broker.config.to.remove").setValue(null), CONFIG_RECORD.highestSupportedVersion())
                ),
                toMap(entry(MYTOPIC, new ApiError(Errors.POLICY_VIOLATION,
                    "Expected: AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={}). Got: " +
                    "AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={foo.bar=123})")),
                entry(BROKER0, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("foo.bar", entry(SET, "123")))),
                entry(BROKER0, toMap(
                        entry("foo.bar", entry(SET, "123")),
                        entry("quux", entry(SET, "456")),
                        entry("broker.config.to.remove", entry(DELETE, null))
                ))),
                true));
    }

    private static class CheckForNullValuesPolicy implements AlterConfigPolicy {
        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            actual.configs().forEach((key, value) -> {
                if (value == null) {
                    throw new PolicyViolationException("Legacy Alter Configs should not see null values");
                }
            });
        }

        @Override
        public void close() {
            // empty
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // empty
        }
    }

    @Test
    public void testLegacyAlterConfigs() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA).
            setAlterConfigPolicy(Optional.of(new CheckForNullValuesPolicy())).
            build();
        List<ApiMessageAndVersion> expectedRecords1 = List.of(
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("abc").setValue("456"), CONFIG_RECORD.highestSupportedVersion()),
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("def").setValue("901"), CONFIG_RECORD.highestSupportedVersion()));
        assertEquals(ControllerResult.atomicOf(
                expectedRecords1, toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(
                toMap(entry(MYTOPIC, toMap(entry("abc", "456"), entry("def", "901")))),
                true));
        for (ApiMessageAndVersion message : expectedRecords1) {
            manager.replay((ConfigRecord) message.message());
        }
        assertEquals(ControllerResult.atomicOf(List.of(
            new ApiMessageAndVersion(
                new ConfigRecord()
                    .setResourceType(TOPIC.id())
                    .setResourceName("mytopic")
                    .setName("abc")
                    .setValue(null),
                CONFIG_RECORD.highestSupportedVersion())),
            toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("def", "901")))),
                true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMaybeGenerateElrSafetyRecords(boolean setStaticConfig) {
        ConfigurationControlManager.Builder builder = new ConfigurationControlManager.Builder().
            setFeatureControl(createFeatureControlManager()).
            setKafkaConfigSchema(SCHEMA);
        if (setStaticConfig) {
            builder.setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"));
        }
        ConfigurationControlManager manager = builder.build();
        Map<String, Entry<AlterConfigOp.OpType, String>> keyToOps =
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, entry(SET, "3")));
        ConfigResource brokerConfigResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ControllerResult<ApiError> result = manager.incrementalAlterConfig(brokerConfigResource, keyToOps, true);
        assertEquals(Set.of(), manager.brokersWithConfigs());

        assertEquals(ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
            new ConfigRecord().setResourceType(BROKER.id()).setResourceName("1").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).setValue("3"), (short) 0)),
            ApiError.NONE), result);

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Set.of(1), manager.brokersWithConfigs());

        List<ApiMessageAndVersion> records = new ArrayList<>();
        String effectiveMinInsync = setStaticConfig ? "2" : "1";
        assertEquals("Generating cluster-level min.insync.replicas of " +
            effectiveMinInsync + ". Removing broker-level min.insync.replicas " +
            "for brokers: 1.", manager.maybeGenerateElrSafetyRecords(records));

        assertEquals(List.of(new ApiMessageAndVersion(
            new ConfigRecord().
                setResourceType(BROKER.id()).
                setResourceName("").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue(effectiveMinInsync), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(BROKER.id()).
                setResourceName("1").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue(null), (short) 0)),
            records);
        RecordTestUtils.replayAll(manager, records);
        assertEquals(Set.of(), manager.brokersWithConfigs());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRejectMinIsrChangeWhenElrEnabled(boolean removal) {
        FeatureControlManager featureManager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                List.of())).
            build();
        featureManager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.LATEST_PRODUCTION.featureLevel()));
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")).
            setFeatureControl(featureManager).
            setKafkaConfigSchema(SCHEMA).
            build();
        ControllerResult<ApiError> result = manager.updateFeatures(
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                FeatureUpdate.UpgradeType.UPGRADE),
            false,
            0);
        assertNotNull(result.response());
        assertEquals(Errors.NONE, result.response().error());
        RecordTestUtils.replayAll(manager, result.records());
        RecordTestUtils.replayAll(featureManager, result.records());

        // Broker level update is not allowed.
        result = manager.incrementalAlterConfig(new ConfigResource(ConfigResource.Type.BROKER, "1"),
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                removal ? entry(DELETE, null) : entry(SET, "3"))),
            true);
        assertEquals(Errors.INVALID_CONFIG, result.response().error());
        assertEquals("Broker-level min.insync.replicas cannot be altered while ELR is enabled.",
            result.response().message());

        // Cluster level removal is not allowed.
        result = manager.incrementalAlterConfig(new ConfigResource(ConfigResource.Type.BROKER, ""),
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                removal ? entry(DELETE, null) : entry(SET, "3"))),
            true);
        if (removal) {
            assertEquals(Errors.INVALID_CONFIG, result.response().error());
            assertEquals("Cluster-level min.insync.replicas cannot be removed while ELR is enabled.",
                    result.response().message());
        } else {
            assertEquals(Errors.NONE, result.response().error());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testElrUpgrade(boolean isMetadataVersionElrEnabled) {
        FeatureControlManager featureManager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                List.of())).
            build();
        featureManager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(isMetadataVersionElrEnabled ? MetadataVersion.IBP_4_0_IV1.featureLevel() : MetadataVersion.IBP_4_0_IV0.featureLevel()));
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")).
            setFeatureControl(featureManager).
            setKafkaConfigSchema(SCHEMA).
            build();
        assertFalse(featureManager.isElrFeatureEnabled());
        ControllerResult<ApiError> result = manager.updateFeatures(
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                FeatureUpdate.UpgradeType.UPGRADE),
            false,
            0);
        assertNotNull(result.response());
        if (isMetadataVersionElrEnabled) {
            assertEquals(Errors.NONE, result.response().error());
            RecordTestUtils.replayAll(manager, result.records());
            RecordTestUtils.replayAll(featureManager, result.records());
            assertTrue(featureManager.isElrFeatureEnabled());
        } else {
            assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        }
    }

    private FeatureControlManager createFeatureControlManager() {
        FeatureControlManager featureControlManager = new FeatureControlManager.Builder().build();
        featureControlManager.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.LATEST_PRODUCTION.featureLevel()));
        return featureControlManager;
    }
}
