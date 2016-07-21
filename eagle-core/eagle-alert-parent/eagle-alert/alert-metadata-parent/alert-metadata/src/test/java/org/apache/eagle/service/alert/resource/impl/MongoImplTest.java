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

import java.util.Date;
import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.impl.MongoMetadataDaoImpl;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

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
    public void test_apis() {
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
}
