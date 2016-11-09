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

package org.apache.eagle.alert.coordination.model.internal;

import org.junit.Assert;
import org.junit.Test;

public class TopologyTest {
    @Test
    public void testTopology() {
        Topology topology = new Topology("test", 2, 3);
        Assert.assertEquals(null, topology.getClusterName());
        Assert.assertEquals("test", topology.getName());
        Assert.assertEquals(null, topology.getPubBoltId());
        Assert.assertEquals(null, topology.getSpoutId());
        Assert.assertEquals(0, topology.getAlertBoltIds().size());
        Assert.assertEquals(1, topology.getAlertParallelism());
        Assert.assertEquals(0, topology.getGroupNodeIds().size());
        Assert.assertEquals(1, topology.getGroupParallelism());
        Assert.assertEquals(3, topology.getNumOfAlertBolt());
        Assert.assertEquals(2, topology.getNumOfGroupBolt());
        Assert.assertEquals(0, topology.getNumOfPublishBolt());
        Assert.assertEquals(1, topology.getNumOfSpout());
        Assert.assertEquals(1, topology.getSpoutParallelism());

        Topology topology1 = new Topology("test", 2, 3);

        Assert.assertFalse(topology1.equals(topology));
        Assert.assertFalse(topology1.hashCode() == topology.hashCode());
        Assert.assertFalse(topology1 == topology);
    }
}
