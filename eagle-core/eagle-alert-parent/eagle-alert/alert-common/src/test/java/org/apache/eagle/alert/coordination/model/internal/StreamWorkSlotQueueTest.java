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

import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamWorkSlotQueueTest {
    @Test
    public void testStreamWorkSlotQueue() {
        StreamGroup streamGroup = new StreamGroup();
        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT10S");
        StreamPartition streamPartition = new StreamPartition();
        List<String> columns = new ArrayList<>();
        columns.add("jobId");
        streamPartition.setColumns(columns);
        streamPartition.setSortSpec(streamSortSpec);
        streamPartition.setStreamId("test");
        streamPartition.setType(StreamPartition.Type.GROUPBY);
        streamGroup.addStreamPartition(streamPartition);
        WorkSlot workSlot = new WorkSlot("setTopologyName", "setBoltId");
        List<WorkSlot> workSlots = new ArrayList<>();
        workSlots.add(workSlot);
        StreamWorkSlotQueue streamWorkSlotQueue = new StreamWorkSlotQueue(streamGroup, false, new HashMap<>(), workSlots);

        Assert.assertTrue(streamWorkSlotQueue.getQueueId().startsWith("SG[test-]"));
        Assert.assertTrue(streamWorkSlotQueue.getDedicateOption().isEmpty());
        Assert.assertEquals(0, streamWorkSlotQueue.getNumberOfGroupBolts());
        Assert.assertEquals(1, streamWorkSlotQueue.getQueueSize());
        Assert.assertTrue(streamWorkSlotQueue.getTopoGroupStartIndex().isEmpty());
        Assert.assertEquals(-1, streamWorkSlotQueue.getTopologyGroupStartIndex(""));
        Assert.assertEquals(workSlot, streamWorkSlotQueue.getWorkingSlots().get(0));

        StreamWorkSlotQueue streamWorkSlotQueue1 = new StreamWorkSlotQueue(streamGroup, false, new HashMap<>(), workSlots);
        Assert.assertFalse(streamWorkSlotQueue.equals(streamWorkSlotQueue1));
        Assert.assertFalse(streamWorkSlotQueue == streamWorkSlotQueue1);
        Assert.assertFalse(streamWorkSlotQueue.hashCode() == streamWorkSlotQueue1.hashCode());
    }
}
