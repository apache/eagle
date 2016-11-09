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

package org.apache.eagle.alert.engine.model;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PartitionedEventTest {
    @Test
    public void testPartitionedEvent() {
        PartitionedEvent partitionedEvent = new PartitionedEvent();
        Assert.assertEquals("PartitionedEvent[partition=null,event=null,key=0", partitionedEvent.toString());

        Object[] data = new Object[]{"namevalue", "hostvalue", "1", 10, 0.1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", 1};
        StreamEvent streamEvent = new StreamEvent("streamId", 1478667686971l, data);

        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT10S");
        StreamPartition streamPartition = new StreamPartition();
        List<String> columns = new ArrayList<>();
        columns.add("jobId");
        streamPartition.setColumns(columns);
        streamPartition.setSortSpec(streamSortSpec);
        streamPartition.setStreamId("test");
        streamPartition.setType(StreamPartition.Type.GROUPBY);

        partitionedEvent = new PartitionedEvent(streamEvent, streamPartition, 1);
        Assert.assertEquals("PartitionedEvent[partition=StreamPartition[streamId=test,type=GROUPBY,columns=[jobId],sortSpec=[StreamSortSpec[windowPeriod=PT10S,windowMargin=30000]]],event=StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10,0.1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},1],metaVersion=null],key=1", partitionedEvent.toString());
        PartitionedEvent partitionedEventCopy = partitionedEvent.copy();
        Assert.assertFalse(partitionedEventCopy == partitionedEvent);
        Assert.assertTrue(partitionedEventCopy.equals(partitionedEvent));
        Assert.assertTrue(partitionedEventCopy.hashCode() == partitionedEvent.hashCode());
    }
}
