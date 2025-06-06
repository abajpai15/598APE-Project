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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.Map;

public class AuthorizerUtils {
    public static Plugin<Authorizer> createAuthorizer(String className, Map<String, Object> configs, Metrics metrics, String key, String role) throws ClassNotFoundException {
        Authorizer authorizer = Utils.newInstance(className, Authorizer.class);
        authorizer.configure(configs);
        return Plugin.wrapInstance(authorizer, metrics, key, "role", role);
    }

    public static boolean isClusterResource(String name) {
        return name.equals(Resource.CLUSTER_NAME);
    }
}
