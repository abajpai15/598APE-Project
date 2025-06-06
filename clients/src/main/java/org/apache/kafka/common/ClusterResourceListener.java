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
package org.apache.kafka.common;

/**
 * A callback interface that users can implement when they wish to get notified about changes in the Cluster metadata.
 * <p>
 * Users who need access to cluster metadata in interceptors, metric reporters, serializers and deserializers
 * can implement this interface. The order of method calls for each of these types is described below.
 * <p>
 * <h4>Clients</h4>
 * There will be one invocation of {@link ClusterResourceListener#onUpdate(ClusterResource)} after each metadata response.
 * <p>
 * {@link org.apache.kafka.clients.producer.ProducerInterceptor} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked after {@link org.apache.kafka.clients.producer.ProducerInterceptor#onSend(org.apache.kafka.clients.producer.ProducerRecord)}
 * but before {@link org.apache.kafka.clients.producer.ProducerInterceptor#onAcknowledgement(org.apache.kafka.clients.producer.RecordMetadata, Exception)} .
 * <p>
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked before {@link org.apache.kafka.clients.consumer.ConsumerInterceptor#onConsume(org.apache.kafka.clients.consumer.ConsumerRecords)}
 * <p>
 * {@link org.apache.kafka.common.serialization.Serializer} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked before {@link org.apache.kafka.common.serialization.Serializer#serialize(String, Object)}
 * <p>
 * {@link org.apache.kafka.common.serialization.Deserializer} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked before {@link org.apache.kafka.common.serialization.Deserializer#deserialize(String, byte[])}
 * <p>
 * {@link org.apache.kafka.common.metrics.MetricsReporter} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked after first {@link org.apache.kafka.clients.producer.KafkaProducer#send(org.apache.kafka.clients.producer.ProducerRecord)} invocation for Producer metrics reporter
 * and after first {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(java.time.Duration)} invocation for Consumer metrics
 * reporters. The reporter may receive metric events from the network layer before this method is invoked.
 * <h4>Broker</h4>
 * There is a single invocation {@link ClusterResourceListener#onUpdate(ClusterResource)} on broker start-up and the cluster metadata will never change.
 * <p>
 * KafkaMetricsReporter : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked during the bootup of the Kafka broker. The reporter may receive metric events from the network layer before this method is invoked.
 * <p>
 * {@link org.apache.kafka.common.metrics.MetricsReporter} : The {@link ClusterResourceListener#onUpdate(ClusterResource)} method will be invoked during the bootup of the Kafka broker. The reporter may receive metric events from the network layer before this method is invoked.
 */
public interface ClusterResourceListener {
    /**
     * A callback method that a user can implement to get updates for {@link ClusterResource}.
     * @param clusterResource cluster metadata
     */
    void onUpdate(ClusterResource clusterResource);
}
