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

package org.apache.eagle.alert.coordination.model;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PolicyWorkerQueueTest {
    @Test
    public void testPolicyWorkerQueue() {

        List<WorkSlot> workers = new ArrayList<>();
        WorkSlot workSlot1 = new WorkSlot("setTopologyName1", "setBoltId1");
        WorkSlot workSlot2 = new WorkSlot("setTopologyName1", "setBoltId2");
        workers.add(workSlot1);
        workers.add(workSlot2);
        PolicyWorkerQueue policyWorkerQueue = new PolicyWorkerQueue(workers);
        Assert.assertEquals(null, policyWorkerQueue.getPartition());
        Assert.assertEquals(workSlot1, policyWorkerQueue.getWorkers().get(0));
        Assert.assertEquals(workSlot2, policyWorkerQueue.getWorkers().get(1));
        Assert.assertEquals("[(setTopologyName1:setBoltId1),(setTopologyName1:setBoltId2)]", policyWorkerQueue.toString());

        PolicyWorkerQueue policyWorkerQueue1 = new PolicyWorkerQueue();
        policyWorkerQueue1.setWorkers(workers);

        Assert.assertTrue(policyWorkerQueue.equals(policyWorkerQueue1));
        Assert.assertTrue(policyWorkerQueue.hashCode() == policyWorkerQueue1.hashCode());

        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT10S");
        StreamPartition streamPartition = new StreamPartition();
        List<String> columns = new ArrayList<>();
        columns.add("jobId");
        streamPartition.setColumns(columns);
        streamPartition.setSortSpec(streamSortSpec);
        streamPartition.setStreamId("test");
        streamPartition.setType(StreamPartition.Type.GROUPBY);
        policyWorkerQueue1.setPartition(streamPartition);

        Assert.assertFalse(policyWorkerQueue.equals(policyWorkerQueue1));
        Assert.assertFalse(policyWorkerQueue.hashCode() == policyWorkerQueue1.hashCode());
    }
}
