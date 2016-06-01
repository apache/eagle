/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.topology.resource.impl;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;


public class TopologyMgmtResourceImplTest {
    TopologyMgmtResourceImpl topologyManager = new TopologyMgmtResourceImpl();
    String topologyName = "testStartTopology";

    @Ignore
    @Test
    public void testStartTopology() throws Exception {
        topologyManager.startTopology(topologyName);
        Thread.sleep(10000);
    }

    @Ignore
    @Test
    public void testStopTopology() throws Exception {
        topologyManager.startTopology(topologyName);
        Thread.sleep(10000);
        topologyManager.stopTopology(topologyName);
    }

    @Ignore
    @Test
    public void testGetTopologies() throws Exception {
        topologyManager.startTopology(topologyName);
        Thread.sleep(10000);
        List<TopologyStatus> topologies = topologyManager.getTopologies();
        Assert.assertTrue(topologies.size() == 1);
    }
}