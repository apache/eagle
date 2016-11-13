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

package org.apache.eagle.alert.engine.coordinator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamPartitionTest {
    @Test
    public void testStreamPartition() {
        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT10S");
        StreamPartition streamPartition = new StreamPartition();
        Assert.assertEquals("StreamPartition[streamId=null,type=null,columns=[],sortSpec=[null]]", streamPartition.toString());
        List<String> columns = new ArrayList<>();
        columns.add("jobId");
        streamPartition.setColumns(columns);
        streamPartition.setSortSpec(streamSortSpec);
        streamPartition.setStreamId("test");
        streamPartition.setType(StreamPartition.Type.GROUPBY);
        Assert.assertEquals("StreamPartition[streamId=test,type=GROUPBY,columns=[jobId],sortSpec=[StreamSortSpec[windowPeriod=PT10S,windowMargin=30000]]]", streamPartition.toString());
        Assert.assertTrue(streamPartition.equals(new StreamPartition(streamPartition)));
        Assert.assertTrue(streamPartition.hashCode() == new StreamPartition(streamPartition).hashCode());
    }
}
