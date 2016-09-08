package org.apache.eagle.alert.engine.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.utils.ByteUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class JavaSerializationTest {
    private final static Logger LOG = LoggerFactory.getLogger(JavaSerializationTest.class);

    @Test
    public void testJavaSerialization() {
        PartitionedEvent partitionedEvent = new PartitionedEvent();
        partitionedEvent.setPartitionKey(partitionedEvent.hashCode());
        partitionedEvent.setPartition(createSampleStreamGroupbyPartition("sampleStream", Arrays.asList("name", "host")));
        StreamEvent event = new StreamEvent();
        event.setStreamId("sampleStream");
        event.setTimestamp(System.currentTimeMillis());
        event.setData(new Object[] {"CPU", "LOCALHOST", true, Long.MAX_VALUE, 60.0});
        partitionedEvent.setEvent(event);

        int javaSerializationLength = SerializationUtils.serialize(partitionedEvent).length;
        LOG.info("Java serialization length: {}, event: {}", javaSerializationLength, partitionedEvent);

        int compactLength = 0;
        compactLength += "sampleStream".getBytes().length;
        compactLength += ByteUtils.intToBytes(partitionedEvent.getPartition().hashCode()).length;
        compactLength += ByteUtils.longToBytes(partitionedEvent.getTimestamp()).length;
        compactLength += "CPU".getBytes().length;
        compactLength += "LOCALHOST".getBytes().length;
        compactLength += 1;
        compactLength += ByteUtils.longToBytes(Long.MAX_VALUE).length;
        compactLength += ByteUtils.doubleToBytes(60.0).length;

        LOG.info("Compact serialization length: {}, event: {}", compactLength, partitionedEvent);
        Assert.assertTrue(compactLength * 20 < javaSerializationLength);
    }


    public static StreamDefinition createSampleStreamDefinition(String streamId) {
        StreamDefinition sampleStreamDefinition = new StreamDefinition();
        sampleStreamDefinition.setStreamId(streamId);
        sampleStreamDefinition.setTimeseries(true);
        sampleStreamDefinition.setValidate(true);
        sampleStreamDefinition.setDescription("Schema for " + streamId);
        List<StreamColumn> streamColumns = new ArrayList<>();

        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        sampleStreamDefinition.setColumns(streamColumns);
        return sampleStreamDefinition;
    }

    public static StreamPartition createSampleStreamGroupbyPartition(String streamId, List<String> groupByField) {
        StreamPartition streamPartition = new StreamPartition();
        streamPartition.setStreamId(streamId);
        streamPartition.setColumns(groupByField);
        streamPartition.setType(StreamPartition.Type.GROUPBY);
        return streamPartition;
    }

    @SuppressWarnings("serial")
    public static PartitionedEvent createSimpleStreamEvent() {
        StreamEvent event = StreamEvent.builder()
            .schema(createSampleStreamDefinition("sampleStream_1"))
            .streamId("sampleStream_1")
            .timestamep(System.currentTimeMillis())
            .attributes(new HashMap<String, Object>() {{
                put("name", "cpu");
                put("host", "localhost");
                put("flag", true);
                put("value", 60.0);
                put("data", Long.MAX_VALUE);
                put("unknown", "unknown column value");
            }}).build();
        PartitionedEvent pEvent = new PartitionedEvent();
        pEvent.setEvent(event);
        pEvent.setPartition(createSampleStreamGroupbyPartition("sampleStream_1", Arrays.asList("name", "host")));
        return pEvent;
    }
}
