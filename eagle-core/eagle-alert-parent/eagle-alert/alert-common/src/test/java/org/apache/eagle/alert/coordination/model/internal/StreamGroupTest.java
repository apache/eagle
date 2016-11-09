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

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamGroupTest {
    @Test
    public void testStreamGroup() {
        StreamGroup streamGroup = new StreamGroup();
        Assert.assertEquals("StreamGroup dedicated=: false partitions=: [] ", streamGroup.toString());
        Assert.assertEquals("SG[]", streamGroup.getStreamId());

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
        Assert.assertEquals("SG[test-]", streamGroup.getStreamId());
        Assert.assertEquals("StreamGroup dedicated=: false partitions=: [StreamPartition[streamId=test,type=GROUPBY,columns=[jobId],sortSpec=[StreamSortSpec[windowPeriod=PT10S,windowMargin=30000]]]] ",
            streamGroup.toString());

        List<StreamPartition> streamPartitions = new ArrayList<>();
        streamPartition.setStreamId("test1");
        streamPartitions.add(streamPartition);
        streamGroup.addStreamPartitions(streamPartitions);
        Assert.assertEquals("SG[test1-test1-]", streamGroup.getStreamId());


        streamPartitions = new ArrayList<>();
        StreamPartition streamPartition1 = new StreamPartition();
        streamPartition1.setStreamId("test2");
        streamPartitions.add(streamPartition1);
        streamGroup.addStreamPartitions(streamPartitions);
        Assert.assertEquals("SG[test1-test1-test2-]", streamGroup.getStreamId());
        Assert.assertEquals("StreamGroup dedicated=: false partitions=: [StreamPartition[streamId=test1,type=GROUPBY,columns=[jobId],sortSpec=[StreamSortSpec[windowPeriod=PT10S,windowMargin=30000]]], StreamPartition[streamId=test1,type=GROUPBY,columns=[jobId],sortSpec=[StreamSortSpec[windowPeriod=PT10S,windowMargin=30000]]], StreamPartition[streamId=test2,type=null,columns=[],sortSpec=[null]]] ", streamGroup.toString());

        StreamGroup streamGroup1 = new StreamGroup();
        streamGroup1.addStreamPartitions(streamGroup.getStreamPartitions());
        Assert.assertTrue(streamGroup.equals(streamGroup1));
        Assert.assertTrue(streamGroup.hashCode() == streamGroup1.hashCode());
    }
}
