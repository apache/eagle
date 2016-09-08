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

package org.apache.eagle.service.alert.resource.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.JdbcMetadataDaoImpl;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class JdbcImplTest {
    private static Logger LOG = LoggerFactory.getLogger(JdbcImplTest.class);
    static IMetadataDao dao;

    @BeforeClass
    public static void setup() {
        System.setProperty("config.resource", "/application-mysql.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load().getConfig("datastore");
        dao = new JdbcMetadataDaoImpl(config);
    }

    @AfterClass
    public static void teardown() {
        if (dao != null) {
            try {
                dao.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String TOPO_NAME = "topoName";

    @Ignore
    @Test
    public void test_apis() {
        // publishment
        {
            Publishment publishment = new Publishment();
            publishment.setName("pub-");
            OpResult result = dao.addPublishment(publishment);
            Assert.assertEquals(200, result.code);
            List<Publishment> assigns = dao.listPublishment();
            Assert.assertEquals(1, assigns.size());
            result = dao.removePublishment("pub-");
            Assert.assertTrue(200 == result.code);
        }
        // topology
        {
            OpResult result = dao.addTopology(new Topology(TOPO_NAME, 3, 5));
            System.out.println(result.message);
            Assert.assertEquals(200, result.code);
            List<Topology> topos = dao.listTopologies();
            Assert.assertEquals(1, topos.size());
            // add again: replace existing one
            result = dao.addTopology(new Topology(TOPO_NAME, 4, 5));
            topos = dao.listTopologies();
            Assert.assertEquals(1, topos.size());
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
        String versionId = "state-" + System.currentTimeMillis();
        state.setVersion(versionId);
        state.setGenerateTime(String.valueOf(new Date().getTime()));
        OpResult result = dao.addScheduleState(state);
        Assert.assertEquals(200, result.code);
        state = dao.getScheduleState();
        Assert.assertEquals(state.getVersion(), versionId);
    }

    @Ignore
    @Test
    public void test_readCurrentState() {
        test_addstate();
        ScheduleState state = dao.getScheduleState();
        Assert.assertNotNull(state);

        LOG.debug(state.getVersion());
        LOG.debug(state.getGenerateTime());
    }
}
