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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;

/**
 * The {@code ValueMapper} interface for mapping a value to a new value of arbitrary type.
 * This is a stateless record-by-record operation, i.e, {@link #apply(Object)} is invoked individually for each record
 * of a stream (cf. {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor} for stateful value transformation).
 * If {@code ValueMapper} is applied to a {@link org.apache.kafka.streams.processor.api.Record} the record's
 * key is preserved.
 * If a record's key and value should be modified {@link KeyValueMapper} can be used.
 *
 * @param <V>  value type
 * @param <VR> mapped value type
 * @see KeyValueMapper
 * @see FixedKeyProcessor
 * @see KStream#mapValues(ValueMapper)
 * @see KStream#mapValues(ValueMapperWithKey)
 * @see KStream#flatMapValues(ValueMapper)
 * @see KStream#flatMapValues(ValueMapperWithKey)
 * @see KTable#mapValues(ValueMapper)
 * @see KTable#mapValues(ValueMapperWithKey)
 */
@FunctionalInterface
public interface ValueMapper<V, VR> {

    /**
     * Map the given value to a new value.
     *
     * @param value the value to be mapped
     * @return the new value
     */
    VR apply(final V value);
}
