/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.StreamRepartitionStrategy;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandlers;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 * Since 5/6/16.
 */
public class TestMetadataSpecSerDeser {
    private String getStreamNameByTopic(String topic) {
        return topic + "Stream";
    }

    @Test
    public void testStreamDefinitions() {
        Map<String, StreamDefinition> sds = new HashMap<>();
        List<String> topics = Arrays.asList("testTopic3", "testTopic4", "testTopic5");
        for (String topic : topics) {
            String streamId = getStreamNameByTopic(topic);
            if (topic.equals("testTopic3") || topic.equals("testTopic4")) {
                StreamDefinition schema = new StreamDefinition();
                schema.setStreamId(streamId);
                StreamColumn column = new StreamColumn();
                column.setName("value");
                column.setType(StreamColumn.Type.STRING);
                schema.setColumns(Collections.singletonList(column));
                sds.put(schema.getStreamId(), schema);
            } else if (topic.equals("testTopic5")) {
                StreamDefinition schema = new StreamDefinition();
                schema.setStreamId(streamId);
                StreamColumn column = new StreamColumn();
                column.setName("value");
                column.setType(StreamColumn.Type.STRING);
                schema.setColumns(Collections.singletonList(column));
                sds.put(schema.getStreamId(), schema);
            }
        }

        String json = MetadataSerDeser.serialize(sds);
        System.out.println(json);

        Map<String, StreamDefinition> deserializedSpec = MetadataSerDeser.deserialize(json, new TypeReference<Map<String, StreamDefinition>>() {
        });
        Assert.assertEquals(3, deserializedSpec.size());
    }

    @SuppressWarnings("unused")
    @Test
    public void testSpoutSpec() {
        String topologyName = "testTopology";
        String spoutId = "alertEngineSpout";
        List<String> plainStringTopics = Arrays.asList("testTopic3", "testTopic4");
        List<String> jsonStringTopics = Arrays.asList("testTopic5");
        Map<String, Kafka2TupleMetadata> kafka2TupleMetadataMap = new HashMap<>();
        for (String topic : plainStringTopics) {
            Kafka2TupleMetadata kafka2TupleMetadata = new Kafka2TupleMetadata();
            kafka2TupleMetadata.setName(topic);
            kafka2TupleMetadata.setTopic(topic);
            kafka2TupleMetadata.setSchemeCls("org.apache.eagle.alert.engine.scheme.PlainStringScheme");
            kafka2TupleMetadataMap.put(topic, kafka2TupleMetadata);
        }
        for (String topic : jsonStringTopics) {
            Kafka2TupleMetadata kafka2TupleMetadata = new Kafka2TupleMetadata();
            kafka2TupleMetadata.setName(topic);
            kafka2TupleMetadata.setTopic(topic);
            kafka2TupleMetadata.setSchemeCls("org.apache.eagle.alert.engine.scheme.JsonScheme");
            kafka2TupleMetadataMap.put(topic, kafka2TupleMetadata);
        }

        // construct Tuple2StreamMetadata
        Map<String, Tuple2StreamMetadata> tuple2StreamMetadataMap = new HashMap<>();
        for (String topic : plainStringTopics) {
            String streamId = getStreamNameByTopic(topic);
            Tuple2StreamMetadata tuple2StreamMetadata = new Tuple2StreamMetadata();
            Set<String> activeStreamNames = new HashSet<>();
            activeStreamNames.add(streamId);
            tuple2StreamMetadata.setStreamNameSelectorCls("org.apache.eagle.alert.engine.scheme.PlainStringStreamNameSelector");
            tuple2StreamMetadata.setStreamNameSelectorProp(new Properties());
            tuple2StreamMetadata.getStreamNameSelectorProp().put("userProvidedStreamName", streamId);
            tuple2StreamMetadata.setActiveStreamNames(activeStreamNames);
            tuple2StreamMetadata.setTimestampColumn("timestamp");
            tuple2StreamMetadataMap.put(topic, tuple2StreamMetadata);
        }

        for (String topic : jsonStringTopics) {
            String streamId = getStreamNameByTopic(topic);
            Tuple2StreamMetadata tuple2StreamMetadata = new Tuple2StreamMetadata();
            Set<String> activeStreamNames = new HashSet<>();
            activeStreamNames.add(streamId);
            tuple2StreamMetadata.setStreamNameSelectorCls("org.apache.eagle.alert.engine.scheme.JsonStringStreamNameSelector");
            tuple2StreamMetadata.setStreamNameSelectorProp(new Properties());
            tuple2StreamMetadata.getStreamNameSelectorProp().put("userProvidedStreamName", streamId);
            tuple2StreamMetadata.setActiveStreamNames(activeStreamNames);
            tuple2StreamMetadata.setTimestampColumn("timestamp");
            tuple2StreamMetadataMap.put(topic, tuple2StreamMetadata);
        }

        // construct StreamRepartitionMetadata
        Map<String, List<StreamRepartitionMetadata>> streamRepartitionMetadataMap = new HashMap<>();
        for (String topic : plainStringTopics) {
            String streamId = getStreamNameByTopic(topic);
            StreamRepartitionMetadata streamRepartitionMetadata = new StreamRepartitionMetadata(topic, "defaultStringStream");
            StreamRepartitionStrategy gs = new StreamRepartitionStrategy();
            // StreamPartition, groupby col1 for stream cpuUsageStream
            StreamPartition sp = new StreamPartition();
            sp.setStreamId(streamId);
            sp.setColumns(Arrays.asList("value"));
            sp.setType(StreamPartition.Type.GROUPBY);
            StreamSortSpec sortSpec = new StreamSortSpec();
            sortSpec.setWindowMargin(1000);
            sortSpec.setWindowPeriod2(Period.seconds(10));
            sp.setSortSpec(sortSpec);

            gs.partition = sp;
            gs.numTotalParticipatingRouterBolts = 1;
            gs.startSequence = 0;
            streamRepartitionMetadata.addGroupStrategy(gs);
            streamRepartitionMetadataMap.put(topic, Arrays.asList(streamRepartitionMetadata));
        }

        for (String topic : jsonStringTopics) {
            String streamId = getStreamNameByTopic(topic);
            StreamRepartitionMetadata streamRepartitionMetadata = new StreamRepartitionMetadata(topic, "defaultStringStream");
            StreamRepartitionStrategy gs = new StreamRepartitionStrategy();
            // StreamPartition, groupby col1 for stream cpuUsageStream
            StreamPartition sp = new StreamPartition();
            sp.setStreamId(streamId);
            sp.setColumns(Arrays.asList("value"));
            sp.setType(StreamPartition.Type.GROUPBY);
            StreamSortSpec sortSpec = new StreamSortSpec();
            sortSpec.setWindowMargin(1000);
            sortSpec.setWindowPeriod2(Period.seconds(10));
            sp.setSortSpec(sortSpec);

            gs.partition = sp;
            gs.numTotalParticipatingRouterBolts = 1;
            gs.startSequence = 0;
            streamRepartitionMetadata.addGroupStrategy(gs);
            streamRepartitionMetadataMap.put(topic, Arrays.asList(streamRepartitionMetadata));
        }

        SpoutSpec newSpec = new SpoutSpec(topologyName, streamRepartitionMetadataMap, tuple2StreamMetadataMap, kafka2TupleMetadataMap);
        String json = MetadataSerDeser.serialize(newSpec);
        System.out.println(json);

        SpoutSpec deserializedSpec = MetadataSerDeser.deserialize(json, SpoutSpec.class);
        Assert.assertNotNull(deserializedSpec);
//        Assert.assertEquals(spoutId, deserializedSpec.getSpoutId());
    }

    @Test
    public void testRouterBoltSpec() {
        List<String> topics = Arrays.asList("testTopic3", "testTopic4", "testTopic5");
        RouterSpec boltSpec = new RouterSpec();
        for (String topic : topics) {
            String streamId = getStreamNameByTopic(topic);
            // StreamPartition, groupby col1 for stream cpuUsageStream
            StreamPartition sp = new StreamPartition();
            sp.setStreamId(streamId);
            sp.setColumns(Arrays.asList("value"));
            sp.setType(StreamPartition.Type.GROUPBY);

            StreamSortSpec sortSpec = new StreamSortSpec();
            sortSpec.setWindowMargin(1000);
            sortSpec.setWindowPeriod2(Period.seconds(10));
            sp.setSortSpec(sortSpec);

            // set StreamRouterSpec to have 2 WorkSlot
            StreamRouterSpec routerSpec = new StreamRouterSpec();
            routerSpec.setPartition(sp);
            routerSpec.setStreamId(streamId);
            PolicyWorkerQueue queue = new PolicyWorkerQueue();
            queue.setPartition(sp);
            queue.setWorkers(Arrays.asList(new WorkSlot("testTopology", "alertBolt0"), new WorkSlot("testTopology", "alertBolt1")));
            routerSpec.setTargetQueue(Arrays.asList(queue));
            boltSpec.addRouterSpec(routerSpec);
        }

        String json = MetadataSerDeser.serialize(boltSpec);
        System.out.println(json);
        RouterSpec deserializedSpec = MetadataSerDeser.deserialize(json, RouterSpec.class);
        Assert.assertEquals(3, deserializedSpec.getRouterSpecs().size());
    }

    @Test
    public void testAlertBoltSpec() {
        String topologyName = "testTopology";
        AlertBoltSpec spec = new AlertBoltSpec();
        List<String> topics = Arrays.asList("testTopic3", "testTopic4", "testTopic5");
        for (String topic : topics) {
            String streamId = getStreamNameByTopic(topic);

            // construct StreamPartition
            StreamPartition sp = new StreamPartition();
            sp.setColumns(Collections.singletonList("value"));
            sp.setStreamId(streamId);
            sp.setType(StreamPartition.Type.GROUPBY);
            StreamSortSpec sortSpec = new StreamSortSpec();
            sortSpec.setWindowMargin(1000);
            sortSpec.setWindowPeriod2(Period.seconds(10));
            sp.setSortSpec(sortSpec);

            spec.setVersion("version1");
            spec.setTopologyName(topologyName);
            PolicyDefinition pd = new PolicyDefinition();
            pd.setName("policy1");
            pd.setPartitionSpec(Collections.singletonList(sp));
            pd.setOutputStreams(Collections.singletonList("testAlertStream"));
            pd.setInputStreams(Collections.singletonList(streamId));
            pd.setDefinition(new PolicyDefinition.Definition());
            pd.getDefinition().type = PolicyStreamHandlers.SIDDHI_ENGINE;
            pd.getDefinition().value = String.format("from %s[value=='xyz'] select value insert into testAlertStream;", streamId);
            spec.addBoltPolicy("alertBolt0", pd.getName());
        }
        String json = MetadataSerDeser.serialize(spec);
        System.out.println(json);
        AlertBoltSpec deserializedSpec = MetadataSerDeser.deserialize(json, AlertBoltSpec.class);
        Assert.assertEquals(topologyName, deserializedSpec.getTopologyName());
    }
}
