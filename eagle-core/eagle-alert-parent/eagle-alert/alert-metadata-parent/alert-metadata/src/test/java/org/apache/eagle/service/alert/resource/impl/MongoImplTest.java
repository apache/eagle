/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.alert.resource.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.MongoMetadataDaoImpl;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @since May 1, 2016
 *
 */
public class MongoImplTest {
    private static Logger LOG = LoggerFactory.getLogger(MongoImplTest.class);
    static IMetadataDao dao;

    private static MongodExecutable mongodExe;
    private static MongodProcess mongod;

    public static void before() {
        try {
            MongodStarter starter = MongodStarter.getDefaultInstance();
            mongodExe = starter.prepare(new MongodConfigBuilder().version(Version.V3_2_1)
                    .net(new Net(27017, Network.localhostIsIPv6())).build());
            mongod = mongodExe.start();
        } catch (Exception e) {
            LOG.error("start embed mongod failed, assume some external mongo running. continue run test!", e);
        }
    }

    @BeforeClass
    public static void setup() {
        before();

        System.setProperty("config.resource", "/application-mongo.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load().getConfig("datastore");
        dao = new MongoMetadataDaoImpl(config);

    }

    @AfterClass
    public static void teardown() {
        if (mongod != null) {
            mongod.stop();
            mongodExe.stop();
        }
    }

    private String TOPO_NAME = "topoName";

    @Test
    public void test_apis() throws Exception {
        // topology
        {
            OpResult result = dao.addTopology(new Topology(TOPO_NAME, 3, 5));
            System.out.println(result.message);
            Assert.assertEquals(200, result.code);
            List<Topology> topos = dao.listTopologies();
            Assert.assertEquals(1, topos.size());

            result = dao.addTopology(new Topology(TOPO_NAME + "-new", 3, 5));
            topos = dao.listTopologies();
            Assert.assertEquals(2, topos.size());
            // add again: replace existing one
            result = dao.addTopology(new Topology(TOPO_NAME, 4, 5));
            topos = dao.listTopologies();
            Assert.assertEquals(2, topos.size());
            Assert.assertEquals(TOPO_NAME, topos.get(0).getName());
            Assert.assertEquals(4, topos.get(0).getNumOfGroupBolt());
        }
        // assignment
        {
            PolicyAssignment assignment = new PolicyAssignment();
            assignment.setPolicyName("policy1");
            OpResult result = dao.addAssignment(assignment);
            Assert.assertEquals(200, result.code);
            List<PolicyAssignment> assigns = dao.listAssignments();
            Assert.assertEquals(1, assigns.size());
        }
        // cluster
        {
            StreamingCluster cluster = new StreamingCluster();
            cluster.setName("dd");
            OpResult result = dao.addCluster(cluster);
            Assert.assertEquals(200, result.code);
            List<StreamingCluster> assigns = dao.listClusters();
            Assert.assertEquals(1, assigns.size());
        }
        // data source
        {
            Kafka2TupleMetadata dataSource = new Kafka2TupleMetadata();
            dataSource.setName("ds");
            OpResult result = dao.addDataSource(dataSource);
            Assert.assertEquals(200, result.code);
            List<Kafka2TupleMetadata> assigns = dao.listDataSources();
            Assert.assertEquals(1, assigns.size());
        }
        // policy
        {
            PolicyDefinition policy = new PolicyDefinition();
            policy.setName("ds");
            OpResult result = dao.addPolicy(policy);
            Assert.assertEquals(200, result.code);
            List<PolicyDefinition> assigns = dao.listPolicies();
            Assert.assertEquals(1, assigns.size());
        }
        // publishment
        {
            Publishment publishment = new Publishment();
            publishment.setName("pub-");
            OpResult result = dao.addPublishment(publishment);
            Assert.assertEquals(200, result.code);
            List<Publishment> assigns = dao.listPublishment();
            Assert.assertEquals(1, assigns.size());
        }
        // publishmentType
        {
            PublishmentType publishmentType = new PublishmentType();
            publishmentType.setType("KAFKA");
            OpResult result = dao.addPublishmentType(publishmentType);
            Assert.assertEquals(200, result.code);
            List<PublishmentType> assigns = dao.listPublishmentType();
            Assert.assertEquals(1, assigns.size());
        }

        // schedule state
        {
            ScheduleState state = new ScheduleState();
            state.setVersion("001");
            state.setScheduleTimeMillis(3000);
            state.setCode(200);
            OpResult result = dao.addScheduleState(state);
            Assert.assertEquals(200, result.code);

            Thread.sleep(1000);

            state = new ScheduleState();
            state.setScheduleTimeMillis(3000);
            state.setVersion("002");
            state.setCode(201);
            result = dao.addScheduleState(state);
            Assert.assertEquals(200, result.code);
            
            ScheduleState getState = dao.getScheduleState();
            Assert.assertEquals(201, getState.getCode());
        }
    }

    private void test_addstate() {
        ScheduleState state = new ScheduleState();
        state.setVersion("state-" + System.currentTimeMillis());
        state.setGenerateTime(String.valueOf(new Date().getTime()));
        OpResult result = dao.addScheduleState(state);
        Assert.assertEquals(200, result.code);
    }

    @Test
    public void test_readCurrentState() {
        test_addstate();
        ScheduleState state = dao.getScheduleState();
        Assert.assertNotNull(state);

        System.out.println(state.getVersion());
        System.out.println(state.getGenerateTime());
    }

    private void test_addCompleteScheduleState() {
        Long timestamp = System.currentTimeMillis();
        String version = "state-" + timestamp;

        // SpoutSpec
        Map<String, SpoutSpec> spoutSpecsMap = new HashMap<>();
        SpoutSpec spoutSpec1 = new SpoutSpec();
        String topologyId1 = "testUnitTopology1_" + timestamp;
        spoutSpec1.setTopologyId(topologyId1);
        spoutSpecsMap.put(topologyId1, spoutSpec1);

        SpoutSpec spoutSpec2 = new SpoutSpec();
        String topologyId2 = "testUnitTopology2_" + timestamp;
        spoutSpec2.setTopologyId(topologyId2);
        spoutSpecsMap.put(topologyId2, spoutSpec2);

        // Alert Spec
        Map<String, AlertBoltSpec> alertSpecsMap = new HashMap<>();
        alertSpecsMap.put(topologyId1, new AlertBoltSpec(topologyId1));

        // GroupSpec
        Map<String, RouterSpec> groupSpecsMap = new HashMap<>();
        groupSpecsMap.put(topologyId1, new RouterSpec(topologyId1));

        // PublishSpec
        Map<String, PublishSpec> pubMap = new HashMap<>();
        pubMap.put(topologyId1, new PublishSpec(topologyId1, "testPublishBolt"));

        // Policy Snapshots
        Collection<PolicyDefinition> policySnapshots = new ArrayList<>();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("testPolicyDefinition");
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("1,jobID,job1,daily_rule,14:00:00,15:00:00");
        def.setType("absencealert");
        policy.setDefinition(def);
        policySnapshots.add(policy);

        // Stream Snapshots
        Collection<StreamDefinition> streams = new ArrayList<>();
        StreamDefinition stream = new StreamDefinition();
        stream.setStreamId("testStream");
        streams.add(stream);

        // Monitored Streams
        Collection<MonitoredStream> monitoredStreams = new ArrayList<>();
        StreamPartition partition = new StreamPartition();
        partition.setType(StreamPartition.Type.GLOBAL);
        partition.setStreamId("s1");
        partition.setColumns(Arrays.asList("f1", "f2"));
        StreamGroup sg = new StreamGroup();
        sg.addStreamPartition(partition);
        MonitoredStream monitoredStream = new MonitoredStream(sg);
        monitoredStreams.add(monitoredStream);

        // Assignments
        Collection<PolicyAssignment> assignments = new ArrayList<>();
        assignments.add(new PolicyAssignment("syslog_regex", "SG[syslog_stream-]"+timestamp));

        ScheduleState state = new ScheduleState(version, spoutSpecsMap, groupSpecsMap, alertSpecsMap, pubMap,
                assignments, monitoredStreams, policySnapshots, streams);

        OpResult result = dao.addScheduleState(state);
        Assert.assertEquals(200, result.code);
    }

    @Test
    public void test_readCompleteScheduleState() {
        test_addCompleteScheduleState();

        ScheduleState state = dao.getScheduleState();
        Assert.assertNotNull(state);
        Assert.assertEquals(2, state.getSpoutSpecs().size());
        Assert.assertEquals(1, state.getAlertSpecs().size());
        Assert.assertEquals(1, state.getGroupSpecs().size());
        Assert.assertEquals(1, state.getPublishSpecs().size());
        Assert.assertEquals(1, state.getPolicySnapshots().size());
        Assert.assertEquals(1, state.getStreamSnapshots().size());
        Assert.assertEquals(1, state.getMonitoredStreams().size());
        Assert.assertEquals(1, state.getAssignments().size());


        System.out.println(state.getVersion());
        System.out.println(state.getGenerateTime());


    }
}
