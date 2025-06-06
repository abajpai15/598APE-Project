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
package org.apache.kafka.test;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class NoOpValueTransformerWithKeySupplier<K, V> implements ValueTransformerWithKeySupplier<K, V, V> {
    public ProcessorContext context;

    @Override
    public ValueTransformerWithKey<K, V, V> get() {
        return new ValueTransformerWithKey<>() {

            @Override
            public void init(final ProcessorContext context1) {
                NoOpValueTransformerWithKeySupplier.this.context = context1;
            }

            @Override
            public V transform(final K readOnlyKey, final V value) {
                return value;
            }

            @Override
            public void close() {
            }
        };
    }
}