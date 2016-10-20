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
package org.apache.eagle.alert.model;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamEventTest {
    @Test
    public void testStreamEventEqual() {
        StreamEvent event1 = mockSimpleStreamEvent();
        StreamEvent event2 = mockSimpleStreamEvent();
        StreamEvent event3 = event2.copy();
        Assert.assertEquals(event1, event2);
        Assert.assertEquals(event2, event3);
    }

    private static StreamEvent mockSimpleStreamEvent() {
        return StreamEvent.builder()
            .schema(mockStreamDefinition("sampleStream_1"))
            .streamId("sampleStream_1")
            .timestamep(System.currentTimeMillis())
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
