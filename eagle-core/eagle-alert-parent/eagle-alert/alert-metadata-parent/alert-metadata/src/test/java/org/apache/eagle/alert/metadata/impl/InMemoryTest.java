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
package org.apache.eagle.alert.metadata.impl;

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.OpResult;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * @since May 1, 2016
 */
public class InMemoryTest {

    private IMetadataDao dao = new InMemMetadataDaoImpl(ConfigFactory.load());

    @Test
    public void test_AddPolicy() {

        LoggerFactory.getLogger(InMemoryTest.class);

        MetadataDaoFactory.getInstance().getMetadataDao();

        PolicyDefinition pd = new PolicyDefinition();
        pd.setName("pd1");
        dao.addPolicy(pd);

        Assert.assertEquals(1, dao.listPolicies().size());
    }

    @Test
    public void testAddCluster(){
        StreamingCluster cluster1 = new StreamingCluster();
        cluster1.setName("test1");
        StreamingCluster cluster2 = new StreamingCluster();
        cluster2.setName("test2");
        StreamingCluster cluster3 = new StreamingCluster();
        cluster3.setName("test2");
        OpResult opResult1 = dao.addCluster(cluster1);
        Assert.assertEquals(OpResult.SUCCESS,opResult1.code);
        OpResult opResult2 = dao.addCluster(cluster2);
        Assert.assertEquals(OpResult.SUCCESS,opResult2.code);
        OpResult opResult3 = dao.addCluster(cluster3);
        Assert.assertEquals(OpResult.SUCCESS,opResult3.code);
        Assert.assertTrue(opResult3.message.contains("replace"));
        dao.clear();
    }

    @Test
    public void testRemoveDataSource(){
        Kafka2TupleMetadata dataSource1 = new Kafka2TupleMetadata();
        Kafka2TupleMetadata dataSource2 = new Kafka2TupleMetadata();
        dataSource1.setName("test1");
        dataSource2.setName("test2");
        dao.addDataSource(dataSource1);
        dao.addDataSource(dataSource2);
        OpResult opResult1 = dao.removeDataSource("test1");
        Assert.assertEquals(OpResult.SUCCESS, opResult1.code);
        OpResult opResult2 = dao.removeDataSource("test1");
        Assert.assertEquals(OpResult.SUCCESS, opResult2.code);
        Assert.assertTrue(opResult2.message.contains("no configuration"));
        dao.clear();
    }

    @Test
    public void testListAlertPublishEvent(){
        dao.addAlertPublishEvent(new AlertPublishEvent());
        dao.addAlertPublishEvent(new AlertPublishEvent());
        Assert.assertEquals(2,dao.listAlertPublishEvent(5).size());
    }

    @Test
    public void testGetAlertPublishEventByPolicyId(){
        AlertPublishEvent alert1 = new AlertPublishEvent();
        AlertPublishEvent alert2 = new AlertPublishEvent();
        alert1.setAlertId("1");
        alert1.setPolicyId("1");
        alert2.setAlertId("2");
        alert2.setPolicyId("1");
        dao.addAlertPublishEvent(alert1);
        dao.addAlertPublishEvent(alert2);
        Assert.assertNotNull(dao.getAlertPublishEvent("1"));
        Assert.assertEquals(2, dao.getAlertPublishEventByPolicyId("1", 2).size());
    }

    @Test
    public void testAddScheduleState(){
        ScheduleState scheduleState = new ScheduleState();
        scheduleState.setVersion("1");
        Assert.assertEquals(OpResult.SUCCESS,dao.addScheduleState(scheduleState).code);
    }

}
