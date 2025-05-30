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

package org.apache.kafka.streams.processor.api;

/**
 * Marker interface for classes implementing {@link ProcessorSupplier}
 * that have been wrapped via a {@link ProcessorWrapper}.
 * <p>
 * To convert a {@link ProcessorSupplier} instance into a {@link WrappedProcessorSupplier},
 * use the {@link ProcessorWrapper#asWrapped(ProcessorSupplier)} method
 */
@FunctionalInterface
public interface WrappedProcessorSupplier<KIn, VIn, KOut, VOut> extends ProcessorSupplier<KIn, VIn, KOut, VOut> {

}
