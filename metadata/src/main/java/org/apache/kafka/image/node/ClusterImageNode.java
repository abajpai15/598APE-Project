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

package org.apache.kafka.image.node;

import org.apache.kafka.image.ClusterImage;

import java.util.Collection;
import java.util.List;


public class ClusterImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "cluster";

    /**
     * The cluster image.
     */
    private final ClusterImage image;

    public ClusterImageNode(ClusterImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return List.of(ClusterImageBrokersNode.NAME, ClusterImageControllersNode.NAME);
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals(ClusterImageBrokersNode.NAME)) {
            return new ClusterImageBrokersNode(image);
        } else if (name.equals(ClusterImageControllersNode.NAME)) {
            return new ClusterImageControllersNode(image);
        } else {
            return null;
        }
    }
}
