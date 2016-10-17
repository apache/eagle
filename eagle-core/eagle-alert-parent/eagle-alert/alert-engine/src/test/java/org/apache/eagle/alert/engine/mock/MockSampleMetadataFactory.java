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
package org.apache.eagle.alert.engine.mock;

import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandlers;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("serial")
public class MockSampleMetadataFactory {
    private static MockStreamMetadataService mockStreamMetadataServiceInstance = null;

    public static MockStreamMetadataService createSingletonMetadataServiceWithSample() {
        if (mockStreamMetadataServiceInstance != null) {
            return mockStreamMetadataServiceInstance;
        }
        mockStreamMetadataServiceInstance = new MockStreamMetadataService();
        mockStreamMetadataServiceInstance.registerStream("sampleStream", createSampleStreamDefinition("sampleStream"));
        mockStreamMetadataServiceInstance.registerStream("sampleStream_1", createSampleStreamDefinition("sampleStream_1"));
        mockStreamMetadataServiceInstance.registerStream("sampleStream_2", createSampleStreamDefinition("sampleStream_2"));
        mockStreamMetadataServiceInstance.registerStream("sampleStream_3", createSampleStreamDefinition("sampleStream_3"));
        mockStreamMetadataServiceInstance.registerStream("sampleStream_4", createSampleStreamDefinition("sampleStream_4"));
        return mockStreamMetadataServiceInstance;
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
        streamColumns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        sampleStreamDefinition.setColumns(streamColumns);
        return sampleStreamDefinition;
    }

    /**
     * By default window period is: PT1m
     *
     * @param streamId
     * @return
     */
    public static StreamSortSpec createSampleStreamSortSpec(String streamId) {
        StreamSortSpec streamSortSpec = new StreamSortSpec();
//        streamSortSpec.setColumn("timestamp");
//        streamSortSpec.setStreamId(streamId);
        streamSortSpec.setWindowMargin(1000);
        streamSortSpec.setWindowPeriod("PT1m");
        return streamSortSpec;
    }

    public static StreamSortSpec createSampleStreamSortSpec(String streamId, String period, int margin) {
        StreamSortSpec streamSortSpec = new StreamSortSpec();
//        streamSortSpec.setColumn("timestamp");
//        streamSortSpec.setStreamId(streamId);
        streamSortSpec.setWindowMargin(margin);
        streamSortSpec.setWindowPeriod(period);
        return streamSortSpec;
    }

    /**
     * Policy: from sampleStream_1[name == "cpu" and value > 50.0] select name, host, flag, value insert into outputStream;
     *
     * @return PolicyDefinition[from sampleStream_1[name == "cpu" and value > 50.0] select name, host, flag, value insert into outputStream;]
     */
    public static PolicyDefinition createSingleMetricSamplePolicy() {
        String definePolicy = "from sampleStream_1[name == \"cpu\" and value > 50.0] select name, host, flag, value insert into outputStream;";
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SamplePolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream_1"));
        policyDefinition.setOutputStreams(Arrays.asList("outputStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(
            PolicyStreamHandlers.SIDDHI_ENGINE,
            definePolicy
        ));
        policyDefinition.setPartitionSpec(Arrays.asList(createSampleStreamGroupbyPartition("sampleStream_1", Arrays.asList("name"))));
        return policyDefinition;
    }

    public static StreamPartition createSampleStreamGroupbyPartition(String streamId, List<String> groupByField) {
        StreamPartition streamPartition = new StreamPartition();
        streamPartition.setStreamId(streamId);
        streamPartition.setColumns(new ArrayList<>(groupByField));
        streamPartition.setType(StreamPartition.Type.GROUPBY);
        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT30m");
        streamSortSpec.setWindowMargin(10000);
        streamPartition.setSortSpec(streamSortSpec);
        return streamPartition;
    }

    public static StreamRouterSpec createSampleStreamRouteSpec(String streamId, String groupByField, List<String> targetEvaluatorIds) {
        List<WorkSlot> slots = Arrays.asList(targetEvaluatorIds.stream().map((t) -> {
            return new WorkSlot("sampleTopology", t);
        }).toArray(WorkSlot[]::new));
        StreamRouterSpec streamRouteSpec = new StreamRouterSpec();
        streamRouteSpec.setStreamId(streamId);
        streamRouteSpec.setPartition(createSampleStreamGroupbyPartition(streamId, Arrays.asList(groupByField)));
        streamRouteSpec.setTargetQueue(Arrays.asList(new PolicyWorkerQueue(slots)));
        return streamRouteSpec;
    }

    public static StreamRouterSpec createSampleStreamRouteSpec(List<String> targetEvaluatorIds) {
        return createSampleStreamRouteSpec("sampleStream_1", "name", targetEvaluatorIds);
    }

    /**
     * GROUPBY_sampleStream_1_ON_name
     *
     * @param targetEvaluatorIds
     * @return
     */
    public static StreamRouterSpec createRouteSpec_GROUP_sampleStream_1_BY_name(List<String> targetEvaluatorIds) {
        return createSampleStreamRouteSpec("sampleStream_1", "name", targetEvaluatorIds);
    }

    public static StreamRouterSpec createRouteSpec_GROUP_sampleStream_2_BY_name(List<String> targetEvaluatorIds) {
        return createSampleStreamRouteSpec("sampleStream_2", "name", targetEvaluatorIds);
    }

    public static PartitionedEvent createSimpleStreamEvent() {
        StreamEvent event = null;
        try {
            event = StreamEvent.builder()
                .schema(MockSampleMetadataFactory.createSingletonMetadataServiceWithSample().getStreamDefinition("sampleStream_1"))
                .streamId("sampleStream_1")
                .timestamep(System.currentTimeMillis())
                .attributes(new HashMap<String, Object>() {{
                    put("name", "cpu");
                    put("value", 60.0);
                    put("unknown", "unknown column value");
                }}).build();
        } catch (StreamNotDefinedException e) {
            e.printStackTrace();
        }
        PartitionedEvent pEvent = new PartitionedEvent();
        pEvent.setEvent(event);
        return pEvent;
    }

    private final static String[] SAMPLE_STREAM_NAME_OPTIONS = new String[] {
        "cpu", "memory", "disk", "network"
    };

    private final static String[] SAMPLE_STREAM_HOST_OPTIONS = new String[] {
        "localhost_1", "localhost_2", "localhost_3", "localhost_4"
    };

    private final static Boolean[] SAMPLE_STREAM_FLAG_OPTIONS = new Boolean[] {
        true, false
    };

    private final static Double[] SAMPLE_STREAM_VALUE_OPTIONS = new Double[] {
        -0.20, 40.4, 50.5, 60.6, 10000.1
    };
    private final static String[] SAMPLE_STREAM_ID_OPTIONS = new String[] {
        "sampleStream_1", "sampleStream_2", "sampleStream_3", "sampleStream_4",
    };
    private final static Random RANDOM = ThreadLocalRandom.current();

    public static StreamEvent createRandomStreamEvent() {
        return createRandomStreamEvent(SAMPLE_STREAM_ID_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_ID_OPTIONS.length)]);
    }

    public static StreamEvent createRandomStreamEvent(String streamId) {
        return createRandomStreamEvent(streamId, System.currentTimeMillis());
    }

    private final static Long[] TIME_DELTA_OPTIONS = new Long[] {
        -30000L, -10000L, -5000L, -1000L, 0L, 1000L, 5000L, 10000L, 30000L
    };

    public static StreamEvent createRandomOutOfTimeOrderStreamEvent(String streamId) {
        StreamEvent event = createRandomStreamEvent(streamId);
        event.setTimestamp(System.currentTimeMillis() + TIME_DELTA_OPTIONS[RANDOM.nextInt(TIME_DELTA_OPTIONS.length)]);
        return event;
    }


    public static PartitionedEvent createRandomOutOfTimeOrderEventGroupedByName(String streamId) {
        StreamEvent event = createRandomStreamEvent(streamId);
        event.setTimestamp(System.currentTimeMillis() + TIME_DELTA_OPTIONS[RANDOM.nextInt(TIME_DELTA_OPTIONS.length)]);
        return new PartitionedEvent(event, createSampleStreamGroupbyPartition(streamId, Arrays.asList("name")), event.getData()[0].hashCode());
    }

    public static PartitionedEvent createPartitionedEventGroupedByName(String streamId, long timestamp) {
        StreamEvent event = createRandomStreamEvent(streamId);
        event.setTimestamp(timestamp);
        return new PartitionedEvent(event, createSampleStreamGroupbyPartition(streamId, Arrays.asList("name")), event.getData()[0].hashCode());
    }

    public static PartitionedEvent createRandomSortedEventGroupedByName(String streamId) {
        StreamEvent event = createRandomStreamEvent(streamId);
        event.setTimestamp(System.currentTimeMillis());
        return new PartitionedEvent(event, createSampleStreamGroupbyPartition(streamId, Arrays.asList("name")), event.getData()[0].hashCode());
    }

    public static StreamEvent createRandomStreamEvent(String streamId, long timestamp) {
        StreamEvent event;
        try {
            event = StreamEvent.builder()
                .schema(MockSampleMetadataFactory.createSingletonMetadataServiceWithSample().getStreamDefinition(streamId))
                .streamId(streamId)
                .timestamep(timestamp)
                .attributes(new HashMap<String, Object>() {{
                    put("name", SAMPLE_STREAM_NAME_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_NAME_OPTIONS.length)]);
                    put("value", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
                    put("host", SAMPLE_STREAM_HOST_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_HOST_OPTIONS.length)]);
                    put("flag", SAMPLE_STREAM_FLAG_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_FLAG_OPTIONS.length)]);
//                        put("value1", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
//                        put("value2", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
//                        put("value3", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
//                        put("value4", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
//                        put("value5", SAMPLE_STREAM_VALUE_OPTIONS[RANDOM.nextInt(SAMPLE_STREAM_VALUE_OPTIONS.length)]);
                    put("unknown", "unknown column value");
                }}).build();
        } catch (StreamNotDefinedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return event;
    }

    public static PartitionedEvent createRandomPartitionedEvent(String streamId, long timestamp) {
        StreamEvent event = createRandomStreamEvent(streamId, timestamp);
        PartitionedEvent partitionedEvent = new PartitionedEvent(event, createSampleStreamGroupbyPartition(streamId, Arrays.asList("name")), event.getData()[0].hashCode());
        return partitionedEvent;
    }
}