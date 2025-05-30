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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.io.Closeable;
import java.util.Map;

/**
 * An interface for converting objects to bytes.
 * A class that implements this interface is expected to have a constructor with no parameter.
 *
 * <p>This interface can be combined with {@link org.apache.kafka.common.ClusterResourceListener ClusterResourceListener}
 * to receive cluster metadata once it's available, as well as {@link org.apache.kafka.common.metrics.Monitorable Monitorable}
 * to enable the serializer to register metrics. For the latter, the following tags are automatically added to all
 * metrics registered: {@code config} set to either {@code key.serializer} or {@code value.serializer},
 * and {@code class} set to the serializer class name.
 *
 * @param <T> Type to be serialized from.
 */
public interface Serializer<T> extends Closeable {

    /**
     * Configure this class.
     *
     * @param configs
     *        configs in key/value pairs
     * @param isKey
     *        whether the serializer is used for the key or the value
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * <p>It is recommended to serialize {@code null} data to the {@code null} byte array.
     *
     * @param topic
     *        topic associated with data
     * @param data
     *        typed data; may be {@code null}
     *
     * @return serialized bytes; may be {@code null}
     */
    byte[] serialize(String topic, T data);

    /**
     * Convert {@code data} into a byte array.
     *
     * <p>It is recommended to serialize {@code null} data to the {@code null} byte array.
     *
     * <p>Note that the passed in {@link Headers} may be empty, but never {@code null}.
     * The implementation is allowed to modify the passed in headers, as a side effect of serialization.
     * It is considered best practice to not delete or modify existing headers, but rather only add new ones.
     *
     * @param topic
     *        topic associated with data
     * @param headers
     *        headers associated with the record
     * @param data
     *        typed data; may be {@code null}
     *
     * @return serialized bytes; may be {@code null}
     */
    default byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }

    /**
     * Close this serializer.
     *
     * <p>This method must be idempotent as it may be called multiple times.
     */
    @Override
    default void close() {
        // intentionally left blank
    }
}
