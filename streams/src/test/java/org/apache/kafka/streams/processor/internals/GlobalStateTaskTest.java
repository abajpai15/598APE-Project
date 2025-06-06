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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.test.GlobalStateManagerStub;
import org.apache.kafka.test.MockProcessorNode;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.NoOpProcessorContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.processor.internals.testutil.ConsumerRecordUtil.record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class GlobalStateTaskTest {

    private final LogContext logContext = new LogContext();

    private final String topic1 = "t1";
    private final String topic2 = "t2";
    private final TopicPartition t1 = new TopicPartition(topic1, 1);
    private final TopicPartition t2 = new TopicPartition(topic2, 1);
    private final MockSourceNode<String, String> sourceOne = new MockSourceNode<>(
        new StringDeserializer(),
        new StringDeserializer());
    private final MockSourceNode<Integer, Integer>  sourceTwo = new MockSourceNode<>(
        new IntegerDeserializer(),
        new IntegerDeserializer());
    private final MockProcessorNode<?, ?, ?, ?> processorOne = new MockProcessorNode<>();
    private final MockProcessorNode<?, ?, ?, ?> processorTwo = new MockProcessorNode<>();

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private final File testDirectory = TestUtils.tempDirectory("global-store");
    private final NoOpProcessorContext context = new NoOpProcessorContext();
    private final MockTime time = new MockTime();
    private final long flushInterval = 1000L;
    private final long currentOffsetT1 = 50;
    private final long currentOffsetT2 = 100;

    private ProcessorTopology topology;
    private GlobalStateManagerStub stateMgr;
    private GlobalStateUpdateTask globalStateTask;

    @BeforeEach
    public void before() {
        final Set<String> storeNames = Set.of("t1-store", "t2-store");
        final Map<String, SourceNode<?, ?>> sourceByTopics = new HashMap<>();
        sourceByTopics.put(topic1, sourceOne);
        sourceByTopics.put(topic2, sourceTwo);
        final Map<String, String> storeToTopic = new HashMap<>();
        storeToTopic.put("t1-store", topic1);
        storeToTopic.put("t2-store", topic2);
        topology = ProcessorTopologyFactories.with(
            asList(sourceOne, sourceTwo, processorOne, processorTwo),
            sourceByTopics,
            Collections.emptyList(),
            storeToTopic);

        offsets.put(t1, 50L);
        offsets.put(t2, 100L);
        stateMgr = new GlobalStateManagerStub(storeNames, offsets, testDirectory);
        globalStateTask = new GlobalStateUpdateTask(
            logContext,
            topology,
            context,
            stateMgr,
            new LogAndFailExceptionHandler(),
            time,
            flushInterval
        );
    }

    @Test
    public void shouldInitializeStateManager() {
        final Map<TopicPartition, Long> startingOffsets = globalStateTask.initialize();
        assertTrue(stateMgr.initialized);
        assertEquals(offsets, startingOffsets);
    }

    @Test
    public void shouldInitializeContext() {
        globalStateTask.initialize();
        assertTrue(context.initialized);
    }

    @Test
    public void shouldInitializeProcessorTopology() {
        globalStateTask.initialize();
        assertTrue(sourceOne.initialized);
        assertTrue(sourceTwo.initialized);
        assertTrue(processorOne.initialized);
        assertTrue(processorTwo.initialized);
    }

    @Test
    public void shouldProcessRecordsForTopic() {
        globalStateTask.initialize();
        globalStateTask.update(record(topic1, 1, 1, "foo".getBytes(), "bar".getBytes()));
        assertEquals(1, sourceOne.numReceived);
        assertEquals(0, sourceTwo.numReceived);
    }

    @Test
    public void shouldProcessRecordsForOtherTopic() {
        final byte[] integerBytes = new IntegerSerializer().serialize("foo", 1);
        globalStateTask.initialize();
        globalStateTask.update(record(topic2, 1, 1, integerBytes, integerBytes));
        assertEquals(1, sourceTwo.numReceived);
        assertEquals(0, sourceOne.numReceived);
    }

    private void maybeDeserialize(final GlobalStateUpdateTask globalStateTask,
                                  final byte[] key,
                                  final byte[] recordValue,
                                  final boolean failExpected) {
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            topic2, 1, 1, 0L, TimestampType.CREATE_TIME,
            0, 0, key, recordValue, new RecordHeaders(), Optional.empty()
        );
        globalStateTask.initialize();
        try {
            globalStateTask.update(record);
            if (failExpected) {
                fail("Should have failed to deserialize.");
            }
        } catch (final StreamsException e) {
            if (!failExpected) {
                fail("Shouldn't have failed to deserialize.");
            }
        }
    }


    @Test
    public void shouldThrowStreamsExceptionWhenKeyDeserializationFails() {
        final byte[] key = new LongSerializer().serialize(topic2, 1L);
        final byte[] recordValue = new IntegerSerializer().serialize(topic2, 10);
        maybeDeserialize(globalStateTask, key, recordValue, true);
    }


    @Test
    public void shouldThrowStreamsExceptionWhenValueDeserializationFails() {
        final byte[] key = new IntegerSerializer().serialize(topic2, 1);
        final byte[] recordValue = new LongSerializer().serialize(topic2, 10L);
        maybeDeserialize(globalStateTask, key, recordValue, true);
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler() {
        final GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
            logContext,
            topology,
            context,
            stateMgr,
            new LogAndContinueExceptionHandler(),
            time,
            flushInterval
        );
        final byte[] key = new LongSerializer().serialize(topic2, 1L);
        final byte[] recordValue = new IntegerSerializer().serialize(topic2, 10);

        maybeDeserialize(globalStateTask2, key, recordValue, false);
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenValueDeserializationFails() {
        final GlobalStateUpdateTask globalStateTask2 = new GlobalStateUpdateTask(
            logContext,
            topology,
            context,
            stateMgr,
            new LogAndContinueExceptionHandler(),
            time,
            flushInterval
        );
        final byte[] key = new IntegerSerializer().serialize(topic2, 1);
        final byte[] recordValue = new LongSerializer().serialize(topic2, 10L);

        maybeDeserialize(globalStateTask2, key, recordValue, false);
    }


    @Test
    public void shouldFlushStateManagerWithOffsets() {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 52L);
        expectedOffsets.put(t2, 100L);

        globalStateTask.initialize();
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 1, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.flushState();

        assertEquals(expectedOffsets, stateMgr.changelogOffsets());
        assertTrue(stateMgr.flushed);
    }

    @Test
    public void shouldCheckpointOffsetsWhenStateIsFlushed() {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 102L);
        expectedOffsets.put(t2, 100L);

        globalStateTask.initialize();
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 51L, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.flushState();

        assertEquals(expectedOffsets, stateMgr.changelogOffsets());
        assertTrue(stateMgr.checkpointWritten);
    }

    @Test
    public void shouldNotCheckpointIfNotReceivedEnoughRecords() {
        globalStateTask.initialize();
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 9000L, "foo".getBytes(), "foo".getBytes()));
        time.sleep(flushInterval); // flush interval elapsed

        stateMgr.checkpointWritten = false;
        stateMgr.flushed = false;

        globalStateTask.maybeCheckpoint();

        assertEquals(offsets, stateMgr.changelogOffsets());
        assertFalse(stateMgr.flushed);
        assertFalse(stateMgr.checkpointWritten);
    }

    @Test
    public void shouldNotCheckpointWhenFlushIntervalHasNotLapsed() {
        globalStateTask.initialize();

        // offset delta exceeded
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 10000L, "foo".getBytes(), "foo".getBytes()));

        time.sleep(flushInterval / 2);

        stateMgr.checkpointWritten = false;
        stateMgr.flushed = false;

        globalStateTask.maybeCheckpoint();

        assertEquals(offsets, stateMgr.changelogOffsets());
        assertFalse(stateMgr.flushed);
        assertFalse(stateMgr.checkpointWritten);
    }

    @Test
    public void shouldCheckpointIfReceivedEnoughRecordsAndFlushIntervalHasElapsed() {
        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 10051L); // topic1 advanced with 10001 records
        expectedOffsets.put(t2, 100L);

        globalStateTask.initialize();

        time.sleep(flushInterval); // flush interval elapsed

        // 10000 records received since last flush => do not flush
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 9999L, "foo".getBytes(), "foo".getBytes()));

        stateMgr.checkpointWritten = false;
        stateMgr.flushed = false;

        globalStateTask.maybeCheckpoint();

        assertEquals(offsets, stateMgr.changelogOffsets());
        assertFalse(stateMgr.flushed);
        assertFalse(stateMgr.checkpointWritten);

        // 1 more record received => triggers the flush
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 10000L, "foo".getBytes(), "foo".getBytes()));
        globalStateTask.maybeCheckpoint();

        assertEquals(expectedOffsets, stateMgr.changelogOffsets());
        assertTrue(stateMgr.flushed);
        assertTrue(stateMgr.checkpointWritten);
    }

    @Test
    public void shouldCheckpointIfReceivedEnoughRecordsFromMultipleTopicsAndFlushIntervalElapsed() {
        final byte[] integerBytes = new IntegerSerializer().serialize(topic2, 1);

        final Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(t1, 9050L); // topic1 advanced with 9000 records
        expectedOffsets.put(t2, 1101L); // topic2 advanced with 1001 records

        globalStateTask.initialize();

        time.sleep(flushInterval);

        // received 9000 records in topic1
        globalStateTask.update(record(topic1, 1, currentOffsetT1 + 8999L, "foo".getBytes(), "foo".getBytes()));
        // received 1001 records in topic2
        globalStateTask.update(record(topic2, 1, currentOffsetT2 + 1000L, integerBytes, integerBytes));
        globalStateTask.maybeCheckpoint();

        assertEquals(expectedOffsets, stateMgr.changelogOffsets());
        assertTrue(stateMgr.flushed);
        assertTrue(stateMgr.checkpointWritten);
    }


    @Test
    public void shouldWipeGlobalStateDirectory() throws Exception {
        assertTrue(stateMgr.baseDir().exists());
        globalStateTask.close(true);
        assertFalse(stateMgr.baseDir().exists());
    }

    @Test
    public void shouldCheckpointDuringInitialization() {
        globalStateTask.initialize();

        assertTrue(stateMgr.checkpointWritten);
        assertTrue(stateMgr.flushed);
    }

    @Test
    public void shouldCheckpointDuringClose() throws Exception {
        globalStateTask.initialize();

        stateMgr.checkpointWritten = false;
        stateMgr.flushed = false;

        globalStateTask.close(false);

        assertTrue(stateMgr.checkpointWritten);
        assertTrue(stateMgr.flushed);
    }
}
