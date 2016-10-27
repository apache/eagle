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

package org.apache.eagle.alert.engine.utils;

import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;

import java.util.*;

public class SpecUtils {


    public static Set<String> getTopicsBySpoutSpec(SpoutSpec spoutSpec) {
        if (spoutSpec == null) {
            return Collections.emptySet();
        }
        return spoutSpec.getKafka2TupleMetadataMap().keySet();
    }

    public static Set<String> getTopicsByConfig(Config config) {
        IMetadataServiceClient client = new MetadataServiceClientImpl(config);
        List<Kafka2TupleMetadata> kafka2TupleMetadata = client.listDataSources();
        Set<String> topics = new HashSet<>();
        for (Kafka2TupleMetadata eachKafka2TupleMetadata : kafka2TupleMetadata) {
            topics.add(eachKafka2TupleMetadata.getTopic());
        }
        return topics;
    }

    public static PublishSpec generatePublishSpec(IMetadataServiceClient mtadataServiceClient) {
        PublishSpec publishSpec = new PublishSpec(Constants.TOPOLOGY_ID, "alertPublishBolt");
        Map<String, List<Publishment>> policyToPub = new HashMap<>();
        List<Publishment> publishments = mtadataServiceClient.listPublishment();
        for (Publishment pub : publishments) {
            for (String policyId : pub.getPolicyIds()) {
                List<Publishment> policyPubs = policyToPub.get(policyId);
                if (policyPubs == null) {
                    policyPubs = new ArrayList<>();
                    policyToPub.put(policyId, policyPubs);
                }
                policyPubs.add(pub);
            }
        }
        List<PolicyDefinition> policyDefinitions = mtadataServiceClient.listPolicies();
        for (PolicyDefinition eachPolicyDefinition : policyDefinitions) {
            String policyId = eachPolicyDefinition.getName();
            if (policyToPub.containsKey(policyId)) {
                for (Publishment pub : policyToPub.get(policyId)) {
                    publishSpec.addPublishment(pub);
                }
            }
        }

        return publishSpec;
    }

    public static Map<String, StreamDefinition> generateSds(IMetadataServiceClient mtadataServiceClient) {
        Map<String, StreamDefinition> streamSchemaMap = new HashMap<>();
        List<StreamDefinition> streamDefinitions = mtadataServiceClient.listStreams();
        for (StreamDefinition eachStreamDefinition : streamDefinitions) {
            streamSchemaMap.put(eachStreamDefinition.getStreamId(), eachStreamDefinition);
        }
        return streamSchemaMap;
    }

    public static AlertBoltSpec generateAlertBoltSpec(IMetadataServiceClient mtadataServiceClient, int numOfAlertBolts) {
        AlertBoltSpec alertSpec = new AlertBoltSpec(Constants.TOPOLOGY_ID);
        Map<String, List<PolicyDefinition>> boltPoliciesMap = new HashMap<>();
        List<PolicyDefinition> policyDefinitions = mtadataServiceClient.listPolicies();
        List<String> alertBolts = generateAlertBolt(numOfAlertBolts);
        for (String alertBolt : alertBolts) {
            boltPoliciesMap.put(alertBolt, policyDefinitions);
        }
        alertSpec.setBoltPoliciesMap(boltPoliciesMap);
        return alertSpec;
    }


    public static RouterSpec generateRouterSpec(IMetadataServiceClient mtadataServiceClient, int numOfAlertBolts) {
        List<StreamWorkSlotQueue> queues = new ArrayList<>();
        List<WorkSlot> workingSlots = new LinkedList<>();
        List<String> alertBolts = generateAlertBolt(numOfAlertBolts);
        for (String alertBolt : alertBolts) {
            workingSlots.add(new WorkSlot(Constants.TOPOLOGY_ID, alertBolt));
        }
        StreamWorkSlotQueue streamWorkSlotQueue = new StreamWorkSlotQueue(new StreamGroup(), false, new HashMap<>(), workingSlots);
        queues.add(streamWorkSlotQueue);
        RouterSpec routerSpec = new RouterSpec(Constants.TOPOLOGY_ID);
        List<PolicyDefinition> policyDefinitions = mtadataServiceClient.listPolicies();
        for (PolicyDefinition eachPolicyDefinition : policyDefinitions) {
            List<StreamPartition> streamPartitions = eachPolicyDefinition.getPartitionSpec();
            for (StreamPartition streamPartition : streamPartitions) {
                StreamRouterSpec routeSpec = new StreamRouterSpec();
                routeSpec.setPartition(streamPartition);
                routeSpec.setStreamId(streamPartition.getStreamId());
                for (StreamWorkSlotQueue sq : queues) {
                    PolicyWorkerQueue queue = new PolicyWorkerQueue();
                    queue.setWorkers(sq.getWorkingSlots());
                    queue.setPartition(streamPartition);
                    routeSpec.addQueue(queue);
                }
                routerSpec.addRouterSpec(routeSpec);
            }

        }


        return routerSpec;
    }

    public static SpoutSpec generateSpout(IMetadataServiceClient mtadataServiceClient, int numOfAlertBolts) {
        // streamId -> StreamDefinition
        Map<String, StreamDefinition> streamSchemaMap = new HashMap<>();
        List<StreamDefinition> streamDefinitions = mtadataServiceClient.listStreams();
        for (StreamDefinition eachStreamDefinition : streamDefinitions) {
            streamSchemaMap.put(eachStreamDefinition.getStreamId(), eachStreamDefinition);
        }
        //generate datasource -> Kafka2TupleMetadata
        Map<String, Kafka2TupleMetadata> datasourceTupleMetadataMap = new HashMap<>();
        // generate topic -> Kafka2TupleMetadata
        Map<String, Kafka2TupleMetadata> kafka2TupleMetadataMap = new HashMap<>();
        // generate topic -> Tuple2StreamMetadata
        Map<String, Tuple2StreamMetadata> tuple2StreamMetadataMap = new HashMap<>();
        // generate topicId -> StreamRepartitionMetadata
        Map<String, List<StreamRepartitionMetadata>> streamRepartitionMetadataMap = new HashMap<>();
        List<Kafka2TupleMetadata> kafka2TupleMetadata = mtadataServiceClient.listDataSources();
        for (Kafka2TupleMetadata eachKafka2TupleMetadata : kafka2TupleMetadata) {
            String topic = eachKafka2TupleMetadata.getTopic();
            datasourceTupleMetadataMap.put(eachKafka2TupleMetadata.getName(), eachKafka2TupleMetadata);
            kafka2TupleMetadataMap.put(topic, eachKafka2TupleMetadata);
            tuple2StreamMetadataMap.put(topic, eachKafka2TupleMetadata.getCodec());
        }
        List<PolicyDefinition> policyDefinitions = mtadataServiceClient.listPolicies();
        for (PolicyDefinition eachPolicyDefinition : policyDefinitions) {
            for (StreamPartition policyStreamPartition : eachPolicyDefinition.getPartitionSpec()) {
                String stream = policyStreamPartition.getStreamId();
                StreamDefinition schema = streamSchemaMap.get(stream);
                String topic = datasourceTupleMetadataMap.get(schema.getDataSource()).getTopic();
                // add stream name to tuple metadata
                if (tuple2StreamMetadataMap.containsKey(topic)) {
                    Tuple2StreamMetadata tupleMetadata = tuple2StreamMetadataMap.get(topic);
                    tupleMetadata.getActiveStreamNames().add(stream);
                }
                // grouping strategy
                StreamRepartitionStrategy gs = new StreamRepartitionStrategy();
                gs.partition = policyStreamPartition;
                gs.numTotalParticipatingRouterBolts = numOfAlertBolts;
                // add to map
                addGroupingStrategy(streamRepartitionMetadataMap, stream, schema, topic, schema.getDataSource(), gs);
            }
        }
        return new SpoutSpec(Constants.TOPOLOGY_ID, streamRepartitionMetadataMap, tuple2StreamMetadataMap, kafka2TupleMetadataMap);
    }

    private static void addGroupingStrategy(Map<String, List<StreamRepartitionMetadata>> streamsMap, String stream,
                                            StreamDefinition schema, String topicName, String datasourceName, StreamRepartitionStrategy gs) {
        List<StreamRepartitionMetadata> dsStreamMeta;
        if (streamsMap.containsKey(topicName)) {
            dsStreamMeta = streamsMap.get(topicName);
        } else {
            dsStreamMeta = new ArrayList<>();
            streamsMap.put(topicName, dsStreamMeta);
        }
        StreamRepartitionMetadata targetSm = null;
        for (StreamRepartitionMetadata sm : dsStreamMeta) {
            if (stream.equalsIgnoreCase(sm.getStreamId())) {
                targetSm = sm;
                break;
            }
        }
        if (targetSm == null) {
            targetSm = new StreamRepartitionMetadata(topicName, schema.getStreamId());
            dsStreamMeta.add(targetSm);
        }
        if (!targetSm.groupingStrategies.contains(gs)) {
            targetSm.addGroupStrategy(gs);
        }
    }

    private static List<String> generateAlertBolt(int numOfAlertBolts) {
        List<String> alertBolts = new ArrayList<>(numOfAlertBolts);
        for (int j = 0; j < numOfAlertBolts; j++) {
            alertBolts.add(Constants.ALERTBOLTNAME_PREFIX + j);
        }
        return alertBolts;
    }
}