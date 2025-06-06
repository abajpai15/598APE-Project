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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.graph.BaseRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.GlobalStoreNode;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.NodesWithRelaxedNullKeyJoinDownstream;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StateStoreNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSourceNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.TableSourceNode;
import org.apache.kafka.streams.kstream.internals.graph.TableSuppressNode;
import org.apache.kafka.streams.kstream.internals.graph.VersionedSemanticsGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.WindowedStreamProcessorNode;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class InternalStreamsBuilder implements InternalNameProvider {

    private static final String TABLE_SOURCE_SUFFIX = "-source";

    final InternalTopologyBuilder internalTopologyBuilder;
    private final AtomicInteger index = new AtomicInteger(0);

    private final AtomicInteger buildPriorityIndex = new AtomicInteger(0);
    private final LinkedHashMap<GraphNode, LinkedHashSet<OptimizableRepartitionNode<?, ?>>> keyChangingOperationsToOptimizableRepartitionNodes = new LinkedHashMap<>();
    private final LinkedHashSet<GraphNode> mergeNodes = new LinkedHashSet<>();
    private final LinkedHashSet<GraphNode> tableSourceNodes = new LinkedHashSet<>();
    private final LinkedHashSet<GraphNode> versionedSemanticsNodes = new LinkedHashSet<>();
    private final LinkedHashSet<TableSuppressNode<?, ?>> tableSuppressNodesNodes = new LinkedHashSet<>();

    private static final String TOPOLOGY_ROOT = "root";
    private static final Logger LOG = LoggerFactory.getLogger(InternalStreamsBuilder.class);

    protected final GraphNode root = new GraphNode(TOPOLOGY_ROOT) {
        @Override
        public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
            // no-op for root node
        }
    };

    public InternalStreamsBuilder(final InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    public <K, V> KStream<K, V> stream(final Collection<String> topics,
                                       final ConsumedInternal<K, V> consumed) {

        final String name = new NamedInternal(consumed.name()).orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
        final StreamSourceNode<K, V> streamSourceNode = new StreamSourceNode<>(name, topics, consumed);

        addGraphNode(root, streamSourceNode);

        return new KStreamImpl<>(name,
                                 consumed.keySerde(),
                                 consumed.valueSerde(),
                                 Collections.singleton(name),
                                 false,
                                 streamSourceNode,
                                 this);
    }

    public <K, V> KStream<K, V> stream(final Pattern topicPattern,
                                       final ConsumedInternal<K, V> consumed) {
        final String name = new NamedInternal(consumed.name()).orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
        final StreamSourceNode<K, V> streamPatternSourceNode = new StreamSourceNode<>(name, topicPattern, consumed);

        addGraphNode(root, streamPatternSourceNode);

        return new KStreamImpl<>(name,
                                 consumed.keySerde(),
                                 consumed.valueSerde(),
                                 Collections.singleton(name),
                                 false,
                                 streamPatternSourceNode,
                                 this);
    }

    public <K, V> KTable<K, V> table(final String topic,
                                     final ConsumedInternal<K, V> consumed,
                                     final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {

        final NamedInternal named = new NamedInternal(consumed.name());

        final String sourceName = named
            .suffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStreamImpl.SOURCE_NAME);

        final String tableSourceName = named
            .orElseGenerateWithPrefix(this, KTableImpl.SOURCE_NAME);

        final KTableSource<K, V> tableSource = new KTableSource<>(materialized);
        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(tableSource, tableSourceName);

        final TableSourceNode<K, V> tableSourceNode = TableSourceNode.<K, V>tableSourceNodeBuilder()
            .withTopic(topic)
            .withSourceName(sourceName)
            .withNodeName(tableSourceName)
            .withConsumedInternal(consumed)
            .withProcessorParameters(processorParameters)
            .build();
        tableSourceNode.setOutputVersioned(materialized.storeSupplier() instanceof VersionedBytesStoreSupplier);

        addGraphNode(root, tableSourceNode);

        return new KTableImpl<>(tableSourceName,
                                consumed.keySerde(),
                                consumed.valueSerde(),
                                Collections.singleton(sourceName),
                                materialized.queryableStoreName(),
                                tableSource,
                                tableSourceNode,
                                this);
    }

    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
                                                 final ConsumedInternal<K, V> consumed,
                                                 final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        if (materialized.storeSupplier() instanceof VersionedBytesStoreSupplier) {
            throw new TopologyException("GlobalTables cannot be versioned.");
        }

        // explicitly disable logging for global stores
        materialized.withLoggingDisabled();

        final NamedInternal named = new NamedInternal(consumed.name());

        final String sourceName = named
                .suffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStreamImpl.SOURCE_NAME);

        final String processorName = named
                .orElseGenerateWithPrefix(this, KTableImpl.SOURCE_NAME);

        final KTableSource<K, V> tableSource = new KTableSource<>(materialized);

        final ProcessorParameters<K, V, ?, ?> processorParameters = new ProcessorParameters<>(tableSource, processorName);

        final TableSourceNode<K, V> tableSourceNode = TableSourceNode.<K, V>tableSourceNodeBuilder()
            .withTopic(topic)
            .isGlobalKTable(true)
            .withSourceName(sourceName)
            .withConsumedInternal(consumed)
            .withProcessorParameters(processorParameters)
            .build();

        addGraphNode(root, tableSourceNode);

        final String storeName = materialized.storeName();
        return new GlobalKTableImpl<>(new KTableSourceValueGetterSupplier<>(storeName), materialized.queryableStoreName());
    }

    @Override
    public String newProcessorName(final String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }

    @Override
    public String newStoreName(final String prefix) {
        return prefix + String.format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
    }

    public synchronized void addStateStore(final StoreFactory builder) {
        addGraphNode(root, new StateStoreNode<>(builder));
    }

    public synchronized <KIn, VIn> void addGlobalStore(final StoreFactory storeFactory,
                                                       final String topic,
                                                       final ConsumedInternal<KIn, VIn> consumed,
                                                       final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier,
                                                       final boolean reprocessOnRestore) {
        // explicitly disable logging for global stores
        storeFactory.withLoggingDisabled();

        final NamedInternal named = new NamedInternal(consumed.name());
        final String sourceName = named.suffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStreamImpl.SOURCE_NAME);
        final String processorName = named.orElseGenerateWithPrefix(this, KTableImpl.SOURCE_NAME);

        final GraphNode globalStoreNode = new GlobalStoreNode<>(
            storeFactory,
            sourceName,
            topic,
            consumed,
            processorName,
            stateUpdateSupplier,
            reprocessOnRestore
        );

        addGraphNode(root, globalStoreNode);
    }

    void addGraphNode(final GraphNode parent,
                      final GraphNode child) {
        Objects.requireNonNull(parent, "parent node can't be null");
        Objects.requireNonNull(child, "child node can't be null");
        parent.addChild(child);
        maybeAddNodeForOptimizationMetadata(child);
        maybeAddNodeForVersionedSemanticsMetadata(child);
    }

    void addGraphNode(final Collection<GraphNode> parents,
                      final GraphNode child) {
        Objects.requireNonNull(parents, "parent node can't be null");
        Objects.requireNonNull(child, "child node can't be null");

        if (parents.isEmpty()) {
            throw new StreamsException("Parent node collection can't be empty");
        }

        for (final GraphNode parent : parents) {
            addGraphNode(parent, child);
        }
    }

    private void maybeAddNodeForOptimizationMetadata(final GraphNode node) {
        node.setBuildPriority(buildPriorityIndex.getAndIncrement());

        if (node.parentNodes().isEmpty() && !node.nodeName().equals(TOPOLOGY_ROOT)) {
            throw new IllegalStateException(
                "Nodes should not have a null parent node.  Name: " + node.nodeName() + " Type: "
                + node.getClass().getSimpleName());
        }

        if (node.isKeyChangingOperation()) {
            keyChangingOperationsToOptimizableRepartitionNodes.put(node, new LinkedHashSet<>());
        } else if (node instanceof OptimizableRepartitionNode) {
            final GraphNode parentNode = getKeyChangingParentNode(node);
            if (parentNode != null) {
                keyChangingOperationsToOptimizableRepartitionNodes.get(parentNode).add((OptimizableRepartitionNode<?, ?>) node);
            }
        } else if (node.isMergeNode()) {
            mergeNodes.add(node);
        } else if (node instanceof TableSourceNode) {
            tableSourceNodes.add(node);
        }
    }

    private void maybeAddNodeForVersionedSemanticsMetadata(final GraphNode node) {
        if (node instanceof VersionedSemanticsGraphNode) {
            versionedSemanticsNodes.add(node);
        }
        if (node instanceof TableSuppressNode) {
            tableSuppressNodesNodes.add((TableSuppressNode<?, ?>) node);
        }
    }

    // use this method for testing only
    public void buildAndOptimizeTopology() {
        buildAndOptimizeTopology(null);
    }

    public void buildAndOptimizeTopology(final Properties props) {
        mergeDuplicateSourceNodes();
        optimizeTopology(props);
        enableVersionedSemantics();

        final PriorityQueue<GraphNode> graphNodePriorityQueue = new PriorityQueue<>(5, Comparator.comparing(GraphNode::buildPriority));

        graphNodePriorityQueue.offer(root);

        while (!graphNodePriorityQueue.isEmpty()) {
            final GraphNode streamGraphNode = graphNodePriorityQueue.remove();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding nodes to topology {} child nodes {}", streamGraphNode, streamGraphNode.children());
            }

            if (streamGraphNode.allParentsWrittenToTopology() && !streamGraphNode.hasWrittenToTopology()) {
                streamGraphNode.writeToTopology(internalTopologyBuilder);
                streamGraphNode.setHasWrittenToTopology(true);
            }

            for (final GraphNode graphNode : streamGraphNode.children()) {
                graphNodePriorityQueue.offer(graphNode);
            }
        }
        internalTopologyBuilder.validateCopartition();

        internalTopologyBuilder.checkUnprovidedNames();

    }

    /**
     * A user can provide either the config OPTIMIZE which means all optimizations rules will be
     * applied or they can provide a list of optimization rules.
     */
    private void optimizeTopology(final Properties props) {
        final Set<String> optimizationConfigs;
        if (props == null || !props.containsKey(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG)) {
            optimizationConfigs = Collections.emptySet();
        } else {
            optimizationConfigs = StreamsConfig.verifyTopologyOptimizationConfigs(
                (String) props.get(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG));
        }
        if (optimizationConfigs.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS)) {
            LOG.debug("Optimizing the Kafka Streams graph for ktable source nodes");
            reuseKTableSourceTopics();
        }
        if (optimizationConfigs.contains(StreamsConfig.MERGE_REPARTITION_TOPICS)) {
            LOG.debug("Optimizing the Kafka Streams graph for repartition nodes");
            mergeRepartitionTopics();
        }
        if (optimizationConfigs.contains(StreamsConfig.SINGLE_STORE_SELF_JOIN)) {
            LOG.debug("Optimizing the Kafka Streams graph for self-joins");
            rewriteSingleStoreSelfJoin(root, new IdentityHashMap<>());
        }
        LOG.debug("Optimizing the Kafka Streams graph for null-key records");
        rewriteRepartitionNodes();
    }

    private void rewriteRepartitionNodes() {
        final Set<BaseRepartitionNode<?, ?>> nodes = new NodesWithRelaxedNullKeyJoinDownstream(root).find();
        for (final BaseRepartitionNode<?, ?> partitionNode : nodes) {
            if (partitionNode.processorParameters() != null) {
                partitionNode.setProcessorParameters(new ProcessorParameters<>(
                    new KStreamFilter<>((k, v) -> k != null, false),
                    partitionNode.processorParameters().processorName()
                ));
            }
        }
    }


    private void mergeDuplicateSourceNodes() {
        final Map<String, StreamSourceNode<?, ?>> topicsToSourceNodes = new HashMap<>();

        // We don't really care about the order here, but since Pattern does not implement equals() we can't rely on
        // a regular HashMap and containsKey(Pattern). But for our purposes it's sufficient to compare the compiled
        // string and flags to determine if two pattern subscriptions can be merged into a single source node
        final Map<Pattern, StreamSourceNode<?, ?>> patternsToSourceNodes =
            new TreeMap<>(Comparator.comparing(Pattern::pattern).thenComparing(Pattern::flags));

        for (final GraphNode graphNode : root.children()) {
            if (graphNode instanceof StreamSourceNode) {
                final StreamSourceNode<?, ?> currentSourceNode = (StreamSourceNode<?, ?>) graphNode;

                if (currentSourceNode.topicPattern().isPresent()) {
                    final Pattern topicPattern = currentSourceNode.topicPattern().get();
                    if (!patternsToSourceNodes.containsKey(topicPattern)) {
                        patternsToSourceNodes.put(topicPattern, currentSourceNode);
                    } else {
                        final StreamSourceNode<?, ?> mainSourceNode = patternsToSourceNodes.get(topicPattern);
                        mainSourceNode.merge(currentSourceNode);
                        root.removeChild(graphNode);
                    }
                } else {
                    if (currentSourceNode.topicNames().isPresent()) {
                        for (final String topic : currentSourceNode.topicNames().get()) {
                            if (!topicsToSourceNodes.containsKey(topic)) {
                                topicsToSourceNodes.put(topic, currentSourceNode);
                            } else {
                                final StreamSourceNode<?, ?> mainSourceNode = topicsToSourceNodes.get(
                                    topic);
                                // TODO we only merge source nodes if the subscribed topic(s) are an exact match, so it's still not
                                // possible to subscribe to topicA in one KStream and topicA + topicB in another. We could achieve
                                // this by splitting these source nodes into one topic per node and routing to the subscribed children
                                if (!mainSourceNode.topicNames()
                                    .equals(currentSourceNode.topicNames())) {
                                    LOG.error(
                                        "Topic {} was found in  subscription for non-equal source nodes {} and {}",
                                        topic, mainSourceNode, currentSourceNode);
                                    throw new TopologyException(
                                        "Two source nodes are subscribed to overlapping but not equal input topics");
                                }
                                mainSourceNode.merge(currentSourceNode);
                                root.removeChild(graphNode);
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * The self-join rewriting can be applied if the StreamStreamJoinNode has a single parent.
     * If the join is a self-join, remove the node KStreamJoinWindow corresponding to the
     * right argument of the join (the "other"). The join node may have multiple siblings but for
     * this rewriting we only care about the ThisKStreamJoinWindow and the OtherKStreamJoinWindow.
     * We iterate over all the siblings to identify these two nodes so that we can remove the
     * latter.
     */
    private void rewriteSingleStoreSelfJoin(
        final GraphNode currentNode, final Map<GraphNode, Boolean> visited) {
        visited.put(currentNode, true);
        if (currentNode instanceof StreamStreamJoinNode && currentNode.parentNodes().size() == 1) {
            final StreamStreamJoinNode<?, ?, ?, ?> joinNode = (StreamStreamJoinNode<?, ?, ?, ?>) currentNode;
            // Remove JoinOtherWindowed node
            final GraphNode parent = joinNode.parentNodes().stream().findFirst().get();
            GraphNode left = null, right = null;
            for (final GraphNode child: parent.children()) {
                if (child instanceof WindowedStreamProcessorNode && child.buildPriority() < joinNode.buildPriority()) {
                    if (child.nodeName().equals(joinNode.thisWindowedStreamProcessorName())) {
                        left = child;
                    } else if (child.nodeName().equals(joinNode.otherWindowedStreamProcessorName())) {
                        right = child;
                    }
                }
            }
            // Sanity check
            if (left != null && right != null && left.buildPriority() < right.buildPriority()) {
                parent.removeChild(right);
                joinNode.setSelfJoin();
            } else {
                throw new IllegalStateException(String.format("Expected the left node %s to have smaller build priority than the right node %s.", left, right));
            }
        }
        for (final GraphNode child: currentNode.children()) {
            if (!visited.containsKey(child)) {
                rewriteSingleStoreSelfJoin(child, visited);
            }
        }
    }

    private void reuseKTableSourceTopics() {
        LOG.debug("Marking KTable source nodes to optimize using source topic for changelogs ");
        tableSourceNodes.forEach(node -> ((TableSourceNode<?, ?>) node).reuseSourceTopicForChangeLog(true));
    }

    private void mergeRepartitionTopics() {
        maybeUpdateKeyChangingRepartitionNodeMap();
        final Iterator<Entry<GraphNode, LinkedHashSet<OptimizableRepartitionNode<?, ?>>>> entryIterator =
            keyChangingOperationsToOptimizableRepartitionNodes.entrySet().iterator();

        while (entryIterator.hasNext()) {
            final Map.Entry<GraphNode, LinkedHashSet<OptimizableRepartitionNode<?, ?>>> entry = entryIterator.next();

            final GraphNode keyChangingNode = entry.getKey();

            if (entry.getValue().isEmpty()) {
                continue;
            }

            final GroupedInternal<?, ?> groupedInternal = new GroupedInternal<>(getRepartitionSerdes(entry.getValue()));

            final String repartitionTopicName = getFirstRepartitionTopicName(entry.getValue());
            //passing in the name of the first repartition topic, re-used to create the optimized repartition topic
            final GraphNode optimizedSingleRepartition = createRepartitionNode(repartitionTopicName,
                                                                               groupedInternal.keySerde(),
                                                                               groupedInternal.valueSerde());

            // re-use parent buildPriority to make sure the single repartition graph node is evaluated before downstream nodes
            optimizedSingleRepartition.setBuildPriority(keyChangingNode.buildPriority());

            for (final OptimizableRepartitionNode<?, ?> repartitionNodeToBeReplaced : entry.getValue()) {

                final GraphNode keyChangingNodeChild = findParentNodeMatching(repartitionNodeToBeReplaced, gn -> gn.parentNodes().contains(keyChangingNode));

                if (keyChangingNodeChild == null) {
                    throw new StreamsException(String.format("Found a null keyChangingChild node for %s", repartitionNodeToBeReplaced));
                }

                LOG.debug("Found the child node of the key changer {} from the repartition {}.", keyChangingNodeChild, repartitionNodeToBeReplaced);

                // need to add children of key-changing node as children of optimized repartition
                // in order to process records from re-partitioning
                optimizedSingleRepartition.addChild(keyChangingNodeChild);

                LOG.debug("Removing {} from {}  children {}", keyChangingNodeChild, keyChangingNode, keyChangingNode.children());
                // now remove children from key-changing node
                keyChangingNode.removeChild(keyChangingNodeChild);

                // now need to get children of repartition node so we can remove repartition node
                final Collection<GraphNode> repartitionNodeToBeReplacedChildren = repartitionNodeToBeReplaced.children();
                final Collection<GraphNode> parentsOfRepartitionNodeToBeReplaced = repartitionNodeToBeReplaced.parentNodes();

                for (final GraphNode repartitionNodeToBeReplacedChild : repartitionNodeToBeReplacedChildren) {
                    for (final GraphNode parentNode : parentsOfRepartitionNodeToBeReplaced) {
                        parentNode.addChild(repartitionNodeToBeReplacedChild);
                    }
                }

                for (final GraphNode parentNode : parentsOfRepartitionNodeToBeReplaced) {
                    parentNode.removeChild(repartitionNodeToBeReplaced);
                }
                repartitionNodeToBeReplaced.clearChildren();

                // if replaced repartition node is part of any copartition group,
                // we need to update it with the new node name so that co-partitioning won't break.
                internalTopologyBuilder.maybeUpdateCopartitionSourceGroups(repartitionNodeToBeReplaced.nodeName(),
                                                                           optimizedSingleRepartition.nodeName());

                LOG.debug("Updated node {} children {}", optimizedSingleRepartition, optimizedSingleRepartition.children());
            }

            keyChangingNode.addChild(optimizedSingleRepartition);
            entryIterator.remove();
        }
    }

    private void maybeUpdateKeyChangingRepartitionNodeMap() {
        final Map<GraphNode, Set<GraphNode>> mergeNodesToKeyChangers = new HashMap<>();
        final Set<GraphNode> mergeNodeKeyChangingParentsToRemove = new HashSet<>();
        for (final GraphNode mergeNode : mergeNodes) {
            mergeNodesToKeyChangers.put(mergeNode, new LinkedHashSet<>());
            final Set<Map.Entry<GraphNode, LinkedHashSet<OptimizableRepartitionNode<?, ?>>>> entrySet = keyChangingOperationsToOptimizableRepartitionNodes.entrySet();
            for (final Map.Entry<GraphNode, LinkedHashSet<OptimizableRepartitionNode<?, ?>>> entry : entrySet) {
                if (mergeNodeHasRepartitionChildren(mergeNode, entry.getValue())) {
                    final GraphNode maybeParentKey = findParentNodeMatching(mergeNode, node -> node.parentNodes().contains(entry.getKey()));
                    if (maybeParentKey != null) {
                        mergeNodesToKeyChangers.get(mergeNode).add(entry.getKey());
                    }
                }
            }
        }

        for (final Map.Entry<GraphNode, Set<GraphNode>> entry : mergeNodesToKeyChangers.entrySet()) {
            final GraphNode mergeKey = entry.getKey();
            final Collection<GraphNode> keyChangingParents = entry.getValue();
            final LinkedHashSet<OptimizableRepartitionNode<?, ?>> repartitionNodes = new LinkedHashSet<>();
            for (final GraphNode keyChangingParent : keyChangingParents) {
                repartitionNodes.addAll(keyChangingOperationsToOptimizableRepartitionNodes.get(keyChangingParent));
                mergeNodeKeyChangingParentsToRemove.add(keyChangingParent);
            }
            keyChangingOperationsToOptimizableRepartitionNodes.put(mergeKey, repartitionNodes);
        }

        for (final GraphNode mergeNodeKeyChangingParent : mergeNodeKeyChangingParentsToRemove) {
            keyChangingOperationsToOptimizableRepartitionNodes.remove(mergeNodeKeyChangingParent);
        }
    }

    private boolean mergeNodeHasRepartitionChildren(final GraphNode mergeNode,
                                                    final LinkedHashSet<OptimizableRepartitionNode<?, ?>> repartitionNodes) {
        return repartitionNodes.stream().allMatch(n -> findParentNodeMatching(n, gn -> gn.parentNodes().contains(mergeNode)) != null);
    }

    private <K, V> OptimizableRepartitionNode<K, V> createRepartitionNode(final String repartitionTopicName,
                                                                          final Serde<K> keySerde,
                                                                          final Serde<V> valueSerde) {

        final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder =
            OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
        KStreamImpl.createRepartitionedSource(
                this,
                keySerde,
                valueSerde,
                repartitionTopicName,
                null,
                repartitionNodeBuilder,
                true
        );

        // ensures setting the repartition topic to the name of the
        // first repartition topic to get merged
        // this may be an auto-generated name or a user specified name
        repartitionNodeBuilder.withRepartitionTopic(repartitionTopicName);

        return repartitionNodeBuilder.build();

    }

    private GraphNode getKeyChangingParentNode(final GraphNode repartitionNode) {
        final GraphNode shouldBeKeyChangingNode = findParentNodeMatching(repartitionNode, n -> n.isKeyChangingOperation() || n.isValueChangingOperation());

        final GraphNode keyChangingNode = findParentNodeMatching(repartitionNode, GraphNode::isKeyChangingOperation);
        if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.equals(keyChangingNode)) {
            return keyChangingNode;
        }
        return null;
    }

    private String getFirstRepartitionTopicName(final Collection<OptimizableRepartitionNode<?, ?>> repartitionNodes) {
        return repartitionNodes.iterator().next().repartitionTopic();
    }

    @SuppressWarnings("unchecked")
    private <K, V> GroupedInternal<K, V> getRepartitionSerdes(final Collection<OptimizableRepartitionNode<?, ?>> repartitionNodes) {
        Serde<K> keySerde = null;
        Serde<V> valueSerde = null;

        for (final OptimizableRepartitionNode<?, ?> repartitionNode : repartitionNodes) {
            if (keySerde == null && repartitionNode.keySerde() != null) {
                keySerde = (Serde<K>) repartitionNode.keySerde();
            }

            if (valueSerde == null && repartitionNode.valueSerde() != null) {
                valueSerde = (Serde<V>) repartitionNode.valueSerde();
            }

            if (keySerde != null && valueSerde != null) {
                break;
            }
        }

        return new GroupedInternal<>(Grouped.with(keySerde, valueSerde));
    }

    private void enableVersionedSemantics() {
        versionedSemanticsNodes.forEach(node -> {
            for (final GraphNode parentNode : node.parentNodes()) {
                if (isVersionedOrVersionedUpstream(parentNode)) {
                    ((VersionedSemanticsGraphNode) node).enableVersionedSemantics(true, parentNode.nodeName());
                }
            }
        });
        tableSuppressNodesNodes.forEach(node -> {
            if (isVersionedUpstream(node)) {
                throw new TopologyException("suppress() is only supported for non-versioned KTables " +
                    "(note that version semantics might be inherited from upstream)");
            }
        });
    }

    /**
     * Seeks up the parent chain to determine whether the input to this node is versioned,
     * i.e., whether the output of its parent(s) is versioned or not
     */
    private boolean isVersionedUpstream(final GraphNode startSeekingNode) {
        if (root.equals(startSeekingNode)) {
            return false;
        }

        for (final GraphNode parentNode : startSeekingNode.parentNodes()) {
            // in order for this node to be versioned, all parents must be versioned
            if (!isVersionedOrVersionedUpstream(parentNode)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Seeks up the parent chain to determine whether the output of this node is versioned.
     */
    private boolean isVersionedOrVersionedUpstream(final GraphNode startSeekingNode) {
        if (startSeekingNode.isOutputVersioned().isPresent()) {
            return startSeekingNode.isOutputVersioned().get();
        }

        return isVersionedUpstream(startSeekingNode);
    }

    private GraphNode findParentNodeMatching(final GraphNode startSeekingNode,
                                             final Predicate<GraphNode> parentNodePredicate) {
        if (parentNodePredicate.test(startSeekingNode)) {
            return startSeekingNode;
        }
        GraphNode foundParentNode = null;

        for (final GraphNode parentNode : startSeekingNode.parentNodes()) {
            if (parentNodePredicate.test(parentNode)) {
                return parentNode;
            }
            foundParentNode = findParentNodeMatching(parentNode, parentNodePredicate);
        }
        return foundParentNode;
    }

    public GraphNode root() {
        return root;
    }

    public InternalTopologyBuilder internalTopologyBuilder() {
        return internalTopologyBuilder;
    }

}
