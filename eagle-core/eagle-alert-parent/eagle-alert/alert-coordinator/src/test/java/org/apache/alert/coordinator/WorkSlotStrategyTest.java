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
package org.apache.alert.coordinator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.alert.coordinator.mock.TestTopologyMgmtService;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.coordinator.impl.WorkQueueBuilder;
import org.apache.eagle.alert.coordinator.impl.strategies.SameTopologySlotStrategy;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.coordinator.provider.InMemScheduleConext;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @since Apr 27, 2016
 *
 */
public class WorkSlotStrategyTest {

    private static final Logger LOG = LoggerFactory.getLogger(WorkSlotStrategyTest.class);

    @Test
    public void test() {
        InMemScheduleConext context = new InMemScheduleConext();

        StreamPartition partition = new StreamPartition();
        partition.setType(StreamPartition.Type.GLOBAL);
        partition.setStreamId("s1");
        partition.setColumns(Arrays.asList("f1", "f2"));
        
        StreamGroup group = new StreamGroup();
        group.addStreamPartition(partition);


        {
            TestTopologyMgmtService mgmtService = new TestTopologyMgmtService(3, 3, "prefix-time1", true);
            SameTopologySlotStrategy strategy = new SameTopologySlotStrategy(context, group, mgmtService);
            List<WorkSlot> slots = strategy.reserveWorkSlots(5, false, new HashMap<String, Object>());
            Assert.assertEquals(0, slots.size());
            Assert.assertEquals(1, context.getTopologies().size());
        }

        {
            TestTopologyMgmtService mgmtService = new TestTopologyMgmtService(5, 5, "prefix-time2", true);
            SameTopologySlotStrategy strategy = new SameTopologySlotStrategy(context, group, mgmtService);
            List<WorkSlot> slots = strategy.reserveWorkSlots(5, false, new HashMap<String, Object>());
            Assert.assertEquals(5, slots.size());
            LOG.info(slots.get(0).getTopologyName());
            Assert.assertEquals(2, context.getTopologies().size());
            Assert.assertEquals(2, context.getTopologyUsages().size());

            // assert all on same topology
            for (WorkSlot ws : slots) {
                Assert.assertEquals(slots.get(0).getTopologyName(), ws.getTopologyName());
            }
            Iterator<TopologyUsage> it = context.getTopologyUsages().values().iterator();
            TopologyUsage usage = it.next();
            for (AlertBoltUsage u : usage.getAlertUsages().values()) {
                Assert.assertTrue(u.getPartitions().size() == 0);
                Assert.assertTrue(u.getQueueSize() == 0);
            }
            // assert
            usage = it.next();
            for (AlertBoltUsage u : usage.getAlertUsages().values()) {
                LOG.info(u.getBoltId());
                Assert.assertTrue(u.getPartitions().size() == 0);
                Assert.assertTrue(u.getBoltId(), u.getQueueSize() == 0);
            }
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void test2_overlap() {
        InMemScheduleConext context = new InMemScheduleConext();

        StreamPartition partition = new StreamPartition();
        partition.setType(StreamPartition.Type.GLOBAL);
        partition.setStreamId("s1");
        partition.setColumns(Arrays.asList("f1", "f2"));
        StreamGroup sg = new StreamGroup();
        sg.addStreamPartition(partition);

        MonitoredStream ms1 = new MonitoredStream(sg);

        TestTopologyMgmtService mgmtService = new TestTopologyMgmtService(5, 5, "prefix-3", true);

        String topo1 = null;
        String bolt1 = null;
        WorkQueueBuilder wrb = new WorkQueueBuilder(context, mgmtService);
        StreamWorkSlotQueue queue = wrb.createQueue(ms1, false, 5, new HashMap<String, Object>());
        {
            Assert.assertEquals(5, queue.getWorkingSlots().size());
            topo1 = queue.getWorkingSlots().get(0).getTopologyName();
            bolt1 = queue.getWorkingSlots().get(0).getBoltId();
            Assert.assertEquals(1, context.getTopologies().size());
            Assert.assertEquals(1, context.getTopologyUsages().size());
            LOG.info(queue.getWorkingSlots().get(0).getTopologyName());
            for (WorkSlot ws : queue.getWorkingSlots()) {
                Assert.assertEquals(topo1, ws.getTopologyName());
            }

            TopologyUsage usage = context.getTopologyUsages().values().iterator().next();
            for (AlertBoltUsage u : usage.getAlertUsages().values()) {
                Assert.assertTrue(u.getPartitions().size() > 0);
                Assert.assertTrue(u.getBoltId(), u.getQueueSize() > 0);
            }
        }

        // second partition
        StreamPartition partition2 = new StreamPartition();
        partition2.setType(StreamPartition.Type.GLOBAL);
        partition2.setStreamId("s2");
        partition2.setColumns(Arrays.asList("f1", "f2"));

        StreamGroup sg2 = new StreamGroup();
        sg2.addStreamPartition(partition2);
        MonitoredStream ms2 = new MonitoredStream(sg2);
        queue = wrb.createQueue(ms2, false, 5, new HashMap<String, Object>());
        {
            Assert.assertEquals(5, queue.getWorkingSlots().size());
            Assert.assertEquals(2, context.getTopologies().size());
            Assert.assertEquals(2, context.getTopologyUsages().size());
            
            String topo2 = queue.getWorkingSlots().get(0).getTopologyName();
            String bolt2 = queue.getWorkingSlots().get(0).getBoltId();
            for (WorkSlot ws : queue.getWorkingSlots()) {
                Assert.assertEquals(topo2, ws.getTopologyName());
            }
            Assert.assertNotEquals(topo1, topo2);
        }
    }
}
