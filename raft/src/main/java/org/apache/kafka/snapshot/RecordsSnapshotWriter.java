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

package org.apache.kafka.snapshot;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.BatchAccumulator.CompletedBatch;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.util.List;
import java.util.Optional;

public final class RecordsSnapshotWriter<T> implements SnapshotWriter<T> {
    private final RawSnapshotWriter snapshot;
    private final BatchAccumulator<T> accumulator;
    private final Time time;

    private RecordsSnapshotWriter(
        RawSnapshotWriter snapshot,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time time,
        Compression compression,
        RecordSerde<T> serde
    ) {
        this.snapshot = snapshot;
        this.time = time;

        this.accumulator = new BatchAccumulator<>(
            snapshot.snapshotId().epoch(),
            0,
            Integer.MAX_VALUE,
            maxBatchSize,
            10, // maxNumberOfBatches
            memoryPool,
            time,
            compression,
            serde
        );
    }

    /**
     * Adds a {@link SnapshotFooterRecord} to the snapshot
     *
     * No more records should be appended to the snapshot after calling this method
     */
    private void finalizeSnapshotWithFooter() {
        SnapshotFooterRecord footerRecord = new SnapshotFooterRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION);
        accumulator.appendSnapshotFooterRecord(footerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public long lastContainedLogOffset() {
        return snapshot.snapshotId().offset() - 1;
    }

    @Override
    public int lastContainedLogEpoch() {
        return snapshot.snapshotId().epoch();
    }

    @Override
    public boolean isFrozen() {
        return snapshot.isFrozen();
    }

    @Override
    public void append(List<T> records) {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = '%s'.",
                snapshot.snapshotId()
            );

            throw new IllegalStateException(message);
        }

        accumulator.append(snapshot.snapshotId().epoch(), records, false);

        if (accumulator.needsDrain(time.milliseconds())) {
            appendBatches(accumulator.drain());
        }
    }

    @Override
    public long freeze() {
        finalizeSnapshotWithFooter();
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
        return snapshot.sizeInBytes();
    }

    @Override
    public void close() {
        snapshot.close();
        accumulator.close();
    }

    private void appendBatches(List<CompletedBatch<T>> batches) {
        try {
            for (CompletedBatch<T> batch : batches) {
                snapshot.append(batch.data);
            }
        } finally {
            batches.forEach(CompletedBatch::release);
        }
    }

    public static final class Builder {
        private long lastContainedLogTimestamp = 0;
        private Compression compression = Compression.NONE;
        private Time time = Time.SYSTEM;
        private int maxBatchSize = 1024;
        private MemoryPool memoryPool = MemoryPool.NONE;
        private KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        private Optional<VoterSet> voterSet = Optional.empty();
        private Optional<RawSnapshotWriter> rawSnapshotWriter = Optional.empty();

        public Builder setLastContainedLogTimestamp(long lastContainedLogTimestamp) {
            this.lastContainedLogTimestamp = lastContainedLogTimestamp;
            return this;
        }

        public Builder setCompression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder setMemoryPool(MemoryPool memoryPool) {
            this.memoryPool = memoryPool;
            return this;
        }

        public Builder setRawSnapshotWriter(RawSnapshotWriter rawSnapshotWriter) {
            this.rawSnapshotWriter = Optional.ofNullable(rawSnapshotWriter);
            return this;
        }

        public Builder setKraftVersion(KRaftVersion kraftVersion) {
            this.kraftVersion = kraftVersion;
            return this;
        }

        public Builder setVoterSet(Optional<VoterSet> voterSet) {
            this.voterSet = voterSet;
            return this;
        }

        public <T> RecordsSnapshotWriter<T> build(RecordSerde<T> serde) {
            if (rawSnapshotWriter.isEmpty()) {
                throw new IllegalStateException("Builder::build called without a RawSnapshotWriter");
            } else if (rawSnapshotWriter.get().sizeInBytes() != 0) {
                throw new IllegalStateException(
                    String.format("Initializing writer with a non-empty snapshot: %s", rawSnapshotWriter.get().snapshotId())
                );
            } else if (kraftVersion == KRaftVersion.KRAFT_VERSION_0 && voterSet.isPresent()) {
                throw new IllegalStateException(
                    String.format("Voter set (%s) not expected when the kraft.version is 0", voterSet.get())
                );
            }

            RecordsSnapshotWriter<T> writer = new RecordsSnapshotWriter<>(
                rawSnapshotWriter.get(),
                maxBatchSize,
                memoryPool,
                time,
                compression,
                serde
            );

            writer.accumulator.appendControlMessages((baseOffset, epoch, compression, buffer) -> {
                long now = time.milliseconds();
                try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                        buffer,
                        RecordBatch.CURRENT_MAGIC_VALUE,
                        compression,
                        TimestampType.CREATE_TIME,
                        baseOffset,
                        now,
                        RecordBatch.NO_PRODUCER_ID,
                        RecordBatch.NO_PRODUCER_EPOCH,
                        RecordBatch.NO_SEQUENCE,
                        false, // isTransactional
                        true,  // isControlBatch
                        epoch,
                        buffer.capacity()
                    )
                ) {
                    builder.appendSnapshotHeaderMessage(
                        now,
                        new SnapshotHeaderRecord()
                            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                            .setLastContainedLogTimestamp(lastContainedLogTimestamp)
                    );

                    if (kraftVersion.isReconfigSupported()) {
                        builder.appendKRaftVersionMessage(
                            now,
                            new KRaftVersionRecord()
                                .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                                .setKRaftVersion(kraftVersion.featureLevel())
                        );

                        voterSet.ifPresent(set -> builder.appendVotersMessage(
                            now,
                            set.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
                        ));
                    }

                    return builder.build();
                }
            });

            return writer;
        }
    }
}
