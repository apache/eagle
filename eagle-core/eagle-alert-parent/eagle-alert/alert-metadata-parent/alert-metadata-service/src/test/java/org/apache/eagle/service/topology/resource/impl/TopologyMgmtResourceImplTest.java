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

import backtype.storm.StormSubmitter;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.InMemMetadataDaoImpl;
import org.apache.eagle.alert.metadata.impl.MetadataDaoFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TopologyMgmtResourceImpl.class, StormSubmitter.class})
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

    @Test
    public void testGetTopologies1() throws Exception {
        IMetadataDao dao = MetadataDaoFactory.getInstance().getMetadataDao();
        TopologyMgmtResourceImpl service = new TopologyMgmtResourceImpl();
        Field daoField = TopologyMgmtResourceImpl.class.getDeclaredField("dao");
        daoField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(daoField, daoField.getModifiers() & ~Modifier.FINAL);
        daoField.set(null, dao);
        // set data
        Topology topology = new Topology("test", 1, 1);
        StreamingCluster cluster =new StreamingCluster();
        dao.clear();
        dao.addTopology(topology);
        dao.addCluster(cluster);
        TopologyMgmtResourceImpl spy = PowerMockito.spy(service);
        PowerMockito.doReturn(new TopologySummary()).when(spy,"getTopologySummery", Mockito.anyCollection(), Mockito.any(Topology.class));
        Assert.assertEquals(1, spy.getTopologies().size());
    }

    @Test
    public void testStartTopology1() throws Exception {
        IMetadataDao dao = MetadataDaoFactory.getInstance().getMetadataDao();
        TopologyMgmtResourceImpl service = new TopologyMgmtResourceImpl();
        Field daoField = TopologyMgmtResourceImpl.class.getDeclaredField("dao");
        daoField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(daoField, daoField.getModifiers() & ~Modifier.FINAL);
        daoField.set(null, dao);
        // set data
        Topology topology = new Topology("test", 1, 1);
        StreamingCluster cluster =new StreamingCluster();
        dao.clear();
        dao.addTopology(topology);
        dao.addCluster(cluster);
        PowerMockito.mockStatic(StormSubmitter.class);
        PowerMockito.doNothing().when(StormSubmitter.class, "submitTopology",Mockito.eq("test"), Mockito.anyMap(), Mockito.any(StormTopology.class));
        TopologyMgmtResourceImpl spy = PowerMockito.spy(service);
        PowerMockito.doReturn(null).when(spy,"createTopology", Mockito.any(Topology.class));
        spy.startTopology("test");
    }

}