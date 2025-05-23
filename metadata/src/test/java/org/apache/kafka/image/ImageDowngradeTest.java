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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.image.writer.UnwritableMetadataException;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ImageDowngradeTest {
    static class MockLossConsumer implements Consumer<UnwritableMetadataException> {
        private final MetadataVersion expectedMetadataVersion;
        private final List<String> losses;

        MockLossConsumer(MetadataVersion expectedMetadataVersion) {
            this.expectedMetadataVersion = expectedMetadataVersion;
            this.losses = new ArrayList<>();
        }

        @Override
        public void accept(UnwritableMetadataException e) {
            assertEquals(expectedMetadataVersion, e.metadataVersion());
            losses.add(e.loss());
        }
    }

    static final List<ApiMessageAndVersion> TEST_RECORDS = List.of(
            new ApiMessageAndVersion(new TopicRecord().
                    setName("foo").
                    setTopicId(Uuid.fromString("5JPuABiJTPu2pQjpZWM6_A")), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord().
                    setTopicId(Uuid.fromString("5JPuABiJTPu2pQjpZWM6_A")).
                    setReplicas(List.of(0, 1)).
                    setIsr(List.of(0, 1)).
                    setLeader(0).
                    setLeaderEpoch(1).
                    setPartitionEpoch(2), (short) 0));

    static ApiMessageAndVersion metadataVersionRecord(MetadataVersion metadataVersion) {
        return new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), (short) 0);
    }

    /**
     * Test downgrading to a MetadataVersion that doesn't support ZK migration.
     */
    @Test
    public void testPreZkMigrationSupportVersion() {
        writeWithExpectedLosses(MetadataVersion.IBP_3_3_IV3,
            List.of(
                "the isMigratingZkBroker state of one or more brokers"),
            List.of(
                metadataVersionRecord(MetadataVersion.IBP_3_4_IV0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().
                    setBrokerId(123).
                    setIncarnationId(Uuid.fromString("XgjKo16hRWeWrTui0iR5Nw")).
                    setBrokerEpoch(456).
                    setRack(null).
                    setFenced(false).
                    setInControlledShutdown(true).
                    setIsMigratingZkBroker(true), (short) 2),
                TEST_RECORDS.get(0),
                TEST_RECORDS.get(1)),
            List.of(
                metadataVersionRecord(MetadataVersion.IBP_3_3_IV3),
                new ApiMessageAndVersion(new RegisterBrokerRecord().
                    setBrokerId(123).
                    setIncarnationId(Uuid.fromString("XgjKo16hRWeWrTui0iR5Nw")).
                    setBrokerEpoch(456).
                    setRack(null).
                    setFenced(false).
                    setInControlledShutdown(true), (short) 1),
                TEST_RECORDS.get(0),
                TEST_RECORDS.get(1))
        );
    }

    @Test
    void testDirectoryAssignmentState() {
        MetadataVersion outputMetadataVersion = MetadataVersion.IBP_3_7_IV0;
        MetadataVersion inputMetadataVersion = outputMetadataVersion;
        PartitionRecord testPartitionRecord = (PartitionRecord) TEST_RECORDS.get(1).message();
        writeWithExpectedLosses(outputMetadataVersion,
            List.of(
                    "the directory assignment state of one or more replicas"),
            List.of(
                metadataVersionRecord(inputMetadataVersion),
                TEST_RECORDS.get(0),
                new ApiMessageAndVersion(
                    testPartitionRecord.duplicate().setDirectories(List.of(
                        Uuid.fromString("c7QfSi6xSIGQVh3Qd5RJxA"),
                        Uuid.fromString("rWaCHejCRRiptDMvW5Xw0g"))),
                    (short) 2)),
            List.of(
                metadataVersionRecord(outputMetadataVersion),
                TEST_RECORDS.get(0),
                new ApiMessageAndVersion(
                    testPartitionRecord.duplicate().setDirectories(List.of()),
                    (short) 0))
        );
    }

    private static void writeWithExpectedLosses(
        MetadataVersion metadataVersion,
        List<String> expectedLosses,
        List<ApiMessageAndVersion> inputs,
        List<ApiMessageAndVersion> expectedOutputs
    ) {
        MockLossConsumer lossConsumer = new MockLossConsumer(metadataVersion);
        MetadataDelta delta = new MetadataDelta.Builder().build();
        RecordTestUtils.replayAll(delta, inputs);
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(metadataVersion).
                setLossHandler(lossConsumer).
                build());
        assertEquals(expectedLosses, lossConsumer.losses, "Failed to get expected metadata losses.");
        assertEquals(expectedOutputs, writer.records(), "Failed to get expected output records.");
    }
}
