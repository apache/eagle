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

public class MonitoredStreamTest {

    @Test
    public void testMonitoredStream() {

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

        MonitoredStream monitoredStream = new MonitoredStream(streamGroup);
        Assert.assertEquals(null, monitoredStream.getVersion());
        Assert.assertTrue(monitoredStream.getQueues().isEmpty());
        Assert.assertEquals(streamGroup, monitoredStream.getStreamGroup());
        monitoredStream.addQueues(streamWorkSlotQueue);
        Assert.assertEquals(streamWorkSlotQueue, monitoredStream.getQueues().get(0));

        MonitoredStream monitoredStream1 = new MonitoredStream(streamGroup);
        Assert.assertTrue(monitoredStream.equals(monitoredStream1));
        Assert.assertTrue(monitoredStream.hashCode() == monitoredStream1.hashCode());

        monitoredStream.removeQueue(streamWorkSlotQueue);
        Assert.assertTrue(monitoredStream.getQueues().isEmpty());

        Assert.assertTrue(monitoredStream.equals(monitoredStream1));
        Assert.assertTrue(monitoredStream.hashCode() == monitoredStream1.hashCode());

    }
}
