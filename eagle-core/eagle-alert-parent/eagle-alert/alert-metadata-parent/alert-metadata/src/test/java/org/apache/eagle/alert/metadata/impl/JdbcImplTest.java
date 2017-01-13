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

package org.apache.eagle.alert.metadata.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.metadata.IMetadataDao;

import org.apache.eagle.alert.metadata.resource.OpResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class JdbcImplTest {
    private static Logger LOG = LoggerFactory.getLogger(JdbcImplTest.class);
    static IMetadataDao dao;

    @BeforeClass
    public static void setup() {
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load("application-jdbc.conf");
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
            dao.addTopology(new Topology(TOPO_NAME, 4, 5));
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
            dao.removeCluster("dd");
            Assert.assertEquals(0, dao.listClusters().size());
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
            publishmentType.setName("KAFKA");
            publishmentType.setType("org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher");
            List<Map<String, String>> fields = new ArrayList<>();
            Map<String, String> field1 = new HashMap<>();
            field1.put("name", "kafka_broker");
            field1.put("value", "sandbox.hortonworks.com:6667");
            Map<String, String> field2 = new HashMap<>();
            field2.put("name", "topic");
            fields.add(field1);
            fields.add(field2);
            publishmentType.setFields(fields);
            OpResult result = dao.addPublishmentType(publishmentType);
            Assert.assertEquals(200, result.code);
            List<PublishmentType> types = dao.listPublishmentType();
            Assert.assertEquals(6, types.size());

            dao.removePublishmentType("KAFKA");
            types = dao.listPublishmentType();
            Assert.assertTrue(types.size() == 5);
        }
    }

    @Test
    public void test_addstate() {
        ScheduleState state = new ScheduleState();
        String versionId = "state-" + System.currentTimeMillis();
        state.setVersion(versionId);
        state.setGenerateTime(String.valueOf(new Date().getTime()));
        OpResult result = dao.addScheduleState(state);
        Assert.assertEquals(200, result.code);
        state = dao.getScheduleState();
        Assert.assertEquals(state.getVersion(), versionId);
    }

    @Test
    public void test_readCurrentState() {
        test_addstate();
        ScheduleState state = dao.getScheduleState();
        Assert.assertNotNull(state);

        LOG.debug(state.getVersion());
        LOG.debug(state.getGenerateTime());
    }

    @Test
    public void test_clearScheduleState() {
        int maxCapacity = 4;
        List<String> reservedOnes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ScheduleState state = new ScheduleState();
            String versionId = "state-" + (System.currentTimeMillis() + i);
            state.setVersion(versionId);
            state.setGenerateTime(String.valueOf(new Date().getTime()));
            dao.addScheduleState(state);
            if (i >= 10 - maxCapacity) {
                reservedOnes.add(versionId);
            }
        }
        dao.clearScheduleState(maxCapacity);
        List<ScheduleState> scheduleStates = dao.listScheduleStates();
        Assert.assertTrue(scheduleStates.size() == maxCapacity);
        List<String> targetOnes = new ArrayList<>();
        scheduleStates.stream().forEach(state -> targetOnes.add(state.getVersion()));
        LOG.info("reservedOne={}",reservedOnes);
        LOG.info("targetOne={}", targetOnes);
        Assert.assertTrue(CollectionUtils.isEqualCollection(reservedOnes, targetOnes));
    }

    @Test
    public void testUpdate() throws SQLException {
        OpResult updateResult;
        // update
        Publishment publishment = new Publishment();
        publishment.setName("pub-");
        publishment.setType("type1");
        updateResult = dao.addPublishment(publishment);
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);

        publishment.setType("type2");
        updateResult = dao.addPublishment(publishment);
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);
        Assert.assertTrue(dao.listPublishment().get(0).getType().equals("type2"));

        // remove
        updateResult = dao.removePublishment("pub-");
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);
        Assert.assertTrue(dao.listPublishment().size() == 0);

        // update alert event
        AlertPublishEvent alert = new AlertPublishEvent();
        String alertId = UUID.randomUUID().toString();
        alert.setAlertTimestamp(System.currentTimeMillis());
        alert.setAlertId(alertId);
        alert.setPolicyId("policyId");
        alert.setPolicyValue("from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX[str:contains(src,'/tmp/test') and ((cmd=='rename' and str:contains(dst, '.Trash')) or cmd=='delete')] select * insert into hdfs_audit_log_enriched_stream_out");
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("siteId", "sandbox");
        alertData.put("policyId", "sample");
        alert.setAlertData(alertData);
        List<String> appIds = new ArrayList<>();
        appIds.add("app1");
        appIds.add("app2");
        alert.setAppIds(appIds);
        updateResult = dao.addAlertPublishEvent(alert);
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);
        AlertPublishEvent event = dao.getAlertPublishEvent(alertId);
        Assert.assertTrue(CollectionUtils.isEqualCollection(appIds, event.getAppIds()));
        Assert.assertTrue(alertData.equals(event.getAlertData()));
    }

    @Test
    public void testUpdatePublishmentsByPolicyId() {
        OpResult updateResult;
        // add publishment
        String policyId = "policy";
        Publishment pub1 = new Publishment();
        pub1.setName("pub1");
        pub1.setType("type1");
        updateResult = dao.addPublishment(pub1);
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);

        Publishment pub2 = new Publishment();
        pub2.setName("pub2");
        pub2.setType("type2");
        updateResult = dao.addPublishment(pub2);
        Assert.assertTrue(updateResult.code == OpResult.SUCCESS);

        // add policy
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName(policyId);
        OpResult result = dao.addPolicy(policy);
        Assert.assertEquals(200, result.code);

        // get publishments by policyId
        List<String> publishmentIds = new ArrayList<>();
        publishmentIds.add("pub1");
        publishmentIds.add("pub2");
        dao.addPublishmentsToPolicy(policyId, publishmentIds);
        List<Publishment> publishments = dao.getPublishmentsByPolicyId(policyId);
        Assert.assertTrue(publishments.size() == 2);

        publishments = dao.listPublishment();
        Assert.assertTrue(publishments.size() == 2);
    }

}
