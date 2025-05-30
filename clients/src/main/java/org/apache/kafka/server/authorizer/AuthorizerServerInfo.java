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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;

import java.util.Collection;

/**
 * Runtime broker configuration metadata provided to authorizers during start up.
 */
public interface AuthorizerServerInfo {

    /**
     * Returns cluster metadata for the broker running this authorizer including cluster id.
     */
    ClusterResource clusterResource();

    /**
     * Returns broker id. This may be a generated broker id if `broker.id` was not configured.
     */
    int brokerId();

    /**
     * Returns endpoints for all listeners including the advertised host and port to which
     * the listener is bound.
     */
    Collection<Endpoint> endpoints();

    /**
     * Returns the inter-broker endpoint. This is one of the endpoints returned by {@link #endpoints()}.
     */
    Endpoint interBrokerEndpoint();

    /**
     * Returns the configured early start listeners.
     */
    Collection<String> earlyStartListeners();
}
