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
import org.apache.eagle.alert.utils.StreamValidationException;
import org.apache.eagle.alert.utils.StreamValidator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StreamEventTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStreamEvent() {
        Object[] data = new Object[]{"namevalue", "hostvalue", "1", 10.0, 1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", 4};
        StreamEvent streamEvent = new StreamEvent("streamId", 1478667686971l, data);

        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10.0,1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},4],metaVersion=null]", streamEvent.toString());

        streamEvent = new StreamEvent("streamId", 1478667686971l, data, "metaVersion");

        Assert.assertEquals("StreamEvent[stream=STREAMID,timestamp=2016-11-09 05:01:26,971,data=[namevalue,hostvalue,1,10.0,1,-0.2,{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"},4],metaVersion=metaVersion]", streamEvent.toString());
        StreamEvent streamEventCopy = streamEvent.copy();
        Assert.assertFalse(streamEventCopy == streamEvent);
        Assert.assertTrue(streamEventCopy.equals(streamEvent));
        Assert.assertTrue(streamEventCopy.hashCode() == streamEvent.hashCode());

        streamEventCopy.setMetaVersion("");
        Assert.assertFalse(streamEventCopy == streamEvent);
        Assert.assertFalse(streamEventCopy.equals(streamEvent));
        Assert.assertFalse(streamEventCopy.hashCode() == streamEvent.hashCode());

        streamEventCopy.copyFrom(streamEvent);

        Assert.assertFalse(streamEventCopy == streamEvent);
        Assert.assertTrue(streamEventCopy.equals(streamEvent));
        Assert.assertTrue(streamEventCopy.hashCode() == streamEvent.hashCode());


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

        Object[] values = streamEvent.getData(streamDefinition, "int", "salary", "flag", "object");
        Assert.assertEquals(4, values[0]);
        Assert.assertEquals(-0.2, values[1]);
        Assert.assertEquals("1", values[2]);
        Assert.assertEquals("{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", values[3]);

        values = streamEvent.getData(streamDefinition, Arrays.asList("int", "data", "flag", "object"));
        Assert.assertEquals(4, values[0]);
        Assert.assertEquals(1, values[1]);
        Assert.assertEquals("1", values[2]);
        Assert.assertEquals("{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", values[3]);
    }


    @Test
    public void testStreamEvent1() {
        thrown.expect(IndexOutOfBoundsException.class);
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
        StreamEvent streamEvent = new StreamEvent();
        streamEvent.setData(new Object[]{"namevalue", "hostvalue", "1", 10.0, 1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}\"", 4});
        streamEvent.getData(streamDefinition, "salary", "isYhd");

    }

    @Test
    public void testStreamEvent2() {
        thrown.expect(IndexOutOfBoundsException.class);
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
        StreamEvent streamEvent = new StreamEvent();
        streamEvent.setData(new Object[]{"namevalue", "hostvalue", "1", 10.0, 1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}\""});
        streamEvent.getData(streamDefinition, "salary", "int");

    }

    @Test
    public void testStreamValidator() throws StreamValidationException {
        StreamDefinition streamDefinition = mockStreamDefinition("TEST_STREAM");
        StreamValidator validator = new StreamValidator(streamDefinition);
        thrown.expect(StreamValidationException.class);
        validator.validateMap(new HashMap<String, Object>() {{
            put("name", "cpu");
            put("value", 60.0);
        }});
    }

    @Test
    public void testStreamEvent3() {
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
        StreamEvent streamEvent = new StreamEvent();
        streamEvent.setData(new Object[]{"namevalue", 1, "flag", 10.0, 0.1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}\"", 1});
        Object[] values = streamEvent.getData(streamDefinition, "value", "host");
        Assert.assertEquals(10.0, values[0]);
        Assert.assertEquals(1, values[1]);
    }

    @Test
    public void testStreamEventEqual() {
        Long timestamp = System.currentTimeMillis();
        StreamEvent event1 = mockSimpleStreamEvent(timestamp);
        StreamEvent event2 = mockSimpleStreamEvent(timestamp);
        StreamEvent event3 = event2.copy();
        Assert.assertEquals(event1, event2);
        Assert.assertEquals(event2, event3);
    }

    private static StreamEvent mockSimpleStreamEvent(Long timestamp) {
        return StreamEvent.builder()
                .schema(mockStreamDefinition("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(timestamp)
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 60.0);
                    put("unknown", "unknown column value");
                }}).build();
    }

    private static StreamDefinition mockStreamDefinition(String streamId) {
        StreamDefinition sampleStreamDefinition = new StreamDefinition();
        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());

        sampleStreamDefinition.setStreamId(streamId);
        sampleStreamDefinition.setTimeseries(true);
        sampleStreamDefinition.setValidate(true);
        sampleStreamDefinition.setDescription("Schema for " + streamId);
        sampleStreamDefinition.setColumns(streamColumns);
        return sampleStreamDefinition;
    }
}