package org.apache.kafka.jmh.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.coordinator.group.OffsetMetadataManager;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.timeline.SnapshotRegistry;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class OffsetFetchBenchmark {
    private static final Time TIME = Time.SYSTEM;

    @Param({"4000"})
    private int partitionCount;

    @Param({"4000"})
    private int transactionCount;

    private static final String GROUP_ID = "my-group-id";
    private static final String TOPIC_NAME = "my-topic-name";

    private OffsetMetadataManager offsetMetadataManager;

    private List<Integer> partitionIndexes;

    @Setup(Level.Trial)
    public void setup() {
        LogContext logContext = new LogContext();
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord()
            .setTopicId(Uuid.randomUuid())
            .setName(TOPIC_NAME));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);

        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);
        when(groupMetadataManager.group(anyString(), anyLong())).thenReturn(group);

        offsetMetadataManager = new OffsetMetadataManager.Builder()
            .withLogContext(logContext)
            .withSnapshotRegistry(snapshotRegistry)
            .withTime(TIME)
            .withGroupMetadataManager(groupMetadataManager)
            .withGroupCoordinatorConfig(mock(GroupCoordinatorConfig.class))
            .withMetadataImage(image)
            .withGroupCoordinatorMetricsShard(mock(GroupCoordinatorMetricsShard.class))
            .build();

        for (int i = 0; i < transactionCount; i++) {
            snapshotRegistry.idempotentCreateSnapshot(i);
            offsetMetadataManager.replay(
                i,
                3193600 + i,
                new OffsetCommitKey()
                    .setGroup(GROUP_ID)
                    .setTopic(TOPIC_NAME)
                    .setPartition(i),
                new OffsetCommitValue()
                    .setOffset(100)
            );
        }

        partitionIndexes = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitionIndexes.add(i);
        }
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void run() {
        offsetMetadataManager.fetchOffsets(
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId(GROUP_ID)
                .setTopics(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestTopics()
                        .setName(TOPIC_NAME)
                        .setPartitionIndexes(partitionIndexes)
                )),
            Long.MAX_VALUE
        );
    }
}