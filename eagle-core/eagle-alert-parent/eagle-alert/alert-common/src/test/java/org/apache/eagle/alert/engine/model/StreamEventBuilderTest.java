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

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamEventBuilderTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStreamEventBuilder() {

        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).build());
        streamColumns.add(new StreamColumn.Builder().name("object").type(StreamColumn.Type.OBJECT).build());
        streamColumns.add(new StreamColumn.Builder().name("int").type(StreamColumn.Type.INT).build());
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);

        StreamEventBuilder streamEventBuilder = new StreamEventBuilder();
        StreamEvent streamEvent = streamEventBuilder.schema(streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[],metaVersion=metaVersion]", streamEvent.toString());
        Object[] data = new Object[]{"namevalue", "hostvalue", "1", 10.0, 1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", 4};
        streamEvent = streamEventBuilder.schema(streamDefinition).attributes(data).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10.0,1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},4],metaVersion=metaVersion]", streamEvent.toString());

        Map<String, Object> mapdata = new HashMap<>();
        mapdata.put("name", "namevalue");
        mapdata.put("host", "hostvalue");
        mapdata.put("flag", "1");
        mapdata.put("value", 10.0);
        mapdata.put("data", 1);
        mapdata.put("salary", -0.2);
        mapdata.put("object", "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}");
        mapdata.put("int", 4);
        StreamEvent streamEvent1 = streamEventBuilder.schema(streamDefinition).attributes(mapdata, streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10.0,1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},4],metaVersion=metaVersion]", streamEvent.toString());

        Assert.assertTrue(streamEvent1 == streamEvent);
        Assert.assertTrue(streamEvent1.equals(streamEvent));
        Assert.assertTrue(streamEvent1.hashCode() == streamEvent.hashCode());

        StreamEventBuilder streamEventBuilder1 = new StreamEventBuilder();
        streamEvent1 = streamEventBuilder1.schema(streamDefinition).attributes(mapdata, streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();

        Assert.assertFalse(streamEvent1 == streamEvent);
        Assert.assertTrue(streamEvent1.equals(streamEvent));
        Assert.assertTrue(streamEvent1.hashCode() == streamEvent.hashCode());
    }

    @Test
    public void testStreamEventBuilder1() {
        thrown.expect(IllegalArgumentException.class);
        StreamEventBuilder streamEventBuilder = new StreamEventBuilder();
        streamEventBuilder.metaVersion("metaVersion").timestamep(1478667686971l).build();
    }

    @Test
    public void testStreamEventBuilder2() {
        StreamEventBuilder streamEventBuilder = new StreamEventBuilder();

        Map<String, Object> mapdata = new HashMap<>();
        mapdata.put("name", "namevalue");
        mapdata.put("host", "hostvalue");
        mapdata.put("flag", "1");
        mapdata.put("value", 10.0);
        mapdata.put("data", 1);
        mapdata.put("salary", -0.2);
        mapdata.put("object", "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}");
        mapdata.put("int", 4);

        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("int").type(StreamColumn.Type.INT).build());
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);

        StreamEvent streamEvent = streamEventBuilder.attributes(mapdata, streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,4],metaVersion=metaVersion]", streamEvent.toString());
    }

    @Test
    public void testStreamEventBuilder3() {
        StreamEventBuilder streamEventBuilder = new StreamEventBuilder();

        Map<String, Object> mapdata = new HashMap<>();
        mapdata.put("name", "namevalue");
        mapdata.put("host", "hostvalue");
        mapdata.put("flag", "1");
        mapdata.put("value", 10.0);
        mapdata.put("data", 1);
        mapdata.put("salary", -0.2);
        mapdata.put("object", "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}");

        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).build());
        streamColumns.add(new StreamColumn.Builder().name("object").type(StreamColumn.Type.OBJECT).build());
        StreamColumn streamColumn = new StreamColumn.Builder().name("int").type(StreamColumn.Type.INT).build();
        streamColumn.setDefaultValue(100);
        streamColumns.add(streamColumn);
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);

        StreamEvent streamEvent = streamEventBuilder.attributes(mapdata, streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10.0,1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},100],metaVersion=metaVersion]", streamEvent.toString());
    }

    @Test
    public void testStreamEventBuilder4() {
        StreamEventBuilder streamEventBuilder = new StreamEventBuilder();

        Map<String, Object> mapdata = new HashMap<>();
        mapdata.put("name", "namevalue");
        mapdata.put("host1", "hostvalue");
        mapdata.put("flag", "1");

        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);

        StreamEvent streamEvent = streamEventBuilder.attributes(mapdata, streamDefinition).streamId("streamId").metaVersion("metaVersion").timestamep(1478667686971l).build();
        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,,1,],metaVersion=metaVersion]", streamEvent.toString());
    }

}
