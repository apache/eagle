/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.coordinator;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class StreamingClusterTest {
    @Test
    public void testStreamingCluster() {
        StreamingCluster cluster = new StreamingCluster();
        cluster.setName("test");
        cluster.setDeployments(new HashMap<>());
        cluster.setDescription("setDescription");
        cluster.setType(StreamingCluster.StreamingType.STORM);
        cluster.setZone("setZone");

        StreamingCluster cluster1 = new StreamingCluster();
        cluster1.setName("test");
        cluster1.setDeployments(new HashMap<>());
        cluster1.setDescription("setDescription");
        cluster1.setType(StreamingCluster.StreamingType.STORM);
        cluster1.setZone("setZone");


        Assert.assertFalse(cluster == cluster1);
        Assert.assertFalse(cluster.equals(cluster1));
        Assert.assertFalse(cluster.hashCode() == cluster1.hashCode());
    }
}
