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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockConsumerTest {
    
    private final MockConsumer<String, String> consumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

    @Test
    public void testSimpleMock() {
        consumer.subscribe(Collections.singleton("test"));
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("test", 0), 0L);
        beginningOffsets.put(new TopicPartition("test", 1), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
        consumer.seek(new TopicPartition("test", 0), 0);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, 0L, TimestampType.CREATE_TIME,
            0, 0, "key1", "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, 0L, TimestampType.CREATE_TIME,
            0, 0, "key2", "value2", new RecordHeaders(), Optional.empty());
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1));
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        final TopicPartition tp = new TopicPartition("test", 0);
        assertEquals(2L, consumer.position(tp));
        assertEquals(1, recs.nextOffsets().size());
        assertEquals(new OffsetAndMetadata(2, Optional.empty(), ""), recs.nextOffsets().get(tp));
        consumer.commitSync();
        assertEquals(2L, consumer.committed(Collections.singleton(tp)).get(tp).offset());
    }

    @Test
    public void testConsumerRecordsIsEmptyWhenReturningNoRecords() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, null));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 1L));
        consumer.seekToEnd(Collections.singleton(partition));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(0, records.count());
        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldNotClearRecordsForPausedPartitions() {
        TopicPartition partition0 = new TopicPartition("test", 0);
        Collection<TopicPartition> testPartitionList = Collections.singletonList(partition0);
        consumer.assign(testPartitionList);
        consumer.addRecord(new ConsumerRecord<>("test", 0, 0, null, null));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition0, 0L));
        consumer.seekToBeginning(testPartitionList);

        consumer.pause(testPartitionList);
        consumer.poll(Duration.ofMillis(1));
        consumer.resume(testPartitionList);
        ConsumerRecords<String, String> recordsSecondPoll = consumer.poll(Duration.ofMillis(1));
        assertEquals(1, recordsSecondPoll.count());
        assertEquals(1, recordsSecondPoll.nextOffsets().size());
        assertEquals(new OffsetAndMetadata(1, Optional.empty(), ""), recordsSecondPoll.nextOffsets().get(new TopicPartition("test", 0)));
    }

    @Test
    public void endOffsetsShouldBeIdempotent() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.updateEndOffsets(Collections.singletonMap(partition, 10L));
        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(10L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        consumer.updateEndOffsets(Collections.singletonMap(partition, 11L));
        // consumer.endOffsets should NOT change the value of end offsets
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
        assertEquals(11L, (long) consumer.endOffsets(Collections.singleton(partition)).get(partition));
    }

    @Test
    public void testDurationBasedOffsetReset() {
        MockConsumer<String, String> consumer = new MockConsumer<>("by_duration:PT1H");
        consumer.subscribe(Collections.singleton("test"));
        consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)));
        HashMap<TopicPartition, Long> durationBasedOffsets = new HashMap<>();
        durationBasedOffsets.put(new TopicPartition("test", 0), 10L);
        durationBasedOffsets.put(new TopicPartition("test", 1), 11L);
        consumer.updateDurationOffsets(durationBasedOffsets);
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 10L, 0L, TimestampType.CREATE_TIME,
                0, 0, "key1", "value1", new RecordHeaders(), Optional.empty());
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 11L, 0L, TimestampType.CREATE_TIME,
                0, 0, "key2", "value2", new RecordHeaders(), Optional.empty());
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        Iterator<ConsumerRecord<String, String>> iter = records.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRebalanceListener() {
        final List<TopicPartition> revoked = new ArrayList<>();
        final List<TopicPartition> assigned = new ArrayList<>();
        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                revoked.clear();
                revoked.addAll(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                assigned.clear();
                assigned.addAll(partitions);
            }
        };


        consumer.subscribe(Collections.singleton("test"), consumerRebalanceListener);
        assertEquals(0, consumer.poll(Duration.ZERO).count());
        List<TopicPartition> topicPartitionList = Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1));
        consumer.rebalance(topicPartitionList);

        assertTrue(revoked.isEmpty());
        assertEquals(2, assigned.size());
        assertTrue(assigned.contains(topicPartitionList.get(0)));
        assertTrue(assigned.contains(topicPartitionList.get(1)));

        consumer.rebalance(Collections.emptyList());
        assertEquals(2, assigned.size());
        assertTrue(revoked.contains(topicPartitionList.get(0)));
        assertTrue(revoked.contains(topicPartitionList.get(1)));

        consumer.rebalance(Collections.singletonList(topicPartitionList.get(0)));
        assertEquals(1, assigned.size());
        assertTrue(assigned.contains(topicPartitionList.get(0)));

        consumer.rebalance(Collections.singletonList(topicPartitionList.get(1)));
        assertEquals(1, assigned.size());
        assertTrue(assigned.contains(topicPartitionList.get(1)));
        assertEquals(1, revoked.size());
        assertTrue(revoked.contains(topicPartitionList.get(0)));
    }
    
    @Test
    public void testRe2JPatternSubscription() {
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((SubscriptionPattern) null));
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(new SubscriptionPattern("")));

        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(pattern, null));

        consumer.subscribe(pattern);
        assertTrue(consumer.subscription().isEmpty());
        // Check that the subscription to pattern was successfully applied in the mock consumer (using a different
        // subscription type should fail)
        assertThrows(IllegalStateException.class, () -> consumer.subscribe(List.of("topic1")));
    }

    @Test
    public void shouldReturnMaxPollRecords() {
        TopicPartition partition = new TopicPartition("test", 0);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));

        IntStream.range(0, 10).forEach(offset -> {
            consumer.addRecord(new ConsumerRecord<>("test", 0, offset, null, null));
        });

        consumer.setMaxPollRecords(2L);

        ConsumerRecords<String, String> records;

        records = consumer.poll(Duration.ofMillis(1));
        assertEquals(2, records.count());

        records = consumer.poll(Duration.ofMillis(1));
        assertEquals(2, records.count());

        consumer.setMaxPollRecords(Long.MAX_VALUE);

        records = consumer.poll(Duration.ofMillis(1));
        assertEquals(6, records.count());

        records = consumer.poll(Duration.ofMillis(1));
        assertTrue(records.isEmpty());
    }

}
