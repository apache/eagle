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

package org.apache.eagle.alert.engine.spark.manager;

import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.utils.Constants;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SpecManager {

    private IMetadataServiceClient mtadataServiceClient;
    private int numOfAlertBolts;
    private List<Kafka2TupleMetadata> kafka2TupleMetadata = new ArrayList<>();
    private List<Publishment> publishments = new ArrayList<>();
    private List<PolicyDefinition> policyDefinitions = new ArrayList<>();
    private List<StreamDefinition> streamDefinitions = new ArrayList<>();
    private static final Logger LOG = LoggerFactory.getLogger(SpecManager.class);

    public SpecManager(Config config, int numOfAlertBolts) {

        this.numOfAlertBolts = numOfAlertBolts;
        try {
            this.mtadataServiceClient = new MetadataServiceClientImpl(config);
            this.kafka2TupleMetadata = mtadataServiceClient.listDataSources();
            this.publishments = mtadataServiceClient.listPublishment();
            this.policyDefinitions = mtadataServiceClient.listPolicies().stream().filter((t) -> t.getPolicyStatus() != PolicyDefinition.PolicyStatus.DISABLED)
                    .collect(Collectors.toList());
            this.streamDefinitions = mtadataServiceClient.listStreams();
        } catch (Exception e) {
            LOG.error("SpecManager error :" + e.getMessage(), e);
        }
    }


    public Set<String> getTopicsByConfig() {
        Set<String> topics = new HashSet<>();
        for (Kafka2TupleMetadata eachKafka2TupleMetadata : kafka2TupleMetadata) {
            topics.add(eachKafka2TupleMetadata.getTopic());
        }
        return topics;
    }

    public PublishSpec generatePublishSpec() {
        PublishSpec publishSpec = new PublishSpec(Constants.TOPOLOGY_ID, "alertPublishBolt");
        Map<String, List<Publishment>> policyToPub = new HashMap<>();
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

    public Map<String, StreamDefinition> generateSds() {
        Map<String, StreamDefinition> streamSchemaMap = new HashMap<>();
        for (StreamDefinition eachStreamDefinition : streamDefinitions) {
            streamSchemaMap.put(eachStreamDefinition.getStreamId(), eachStreamDefinition);
        }
        return streamSchemaMap;
    }

    public AlertBoltSpec generateAlertBoltSpec() {
        AlertBoltSpec alertSpec = new AlertBoltSpec(Constants.TOPOLOGY_ID);
        Map<String, List<PolicyDefinition>> boltPoliciesMap = new HashMap<>();
        List<String> alertBolts = generateAlertBolt(numOfAlertBolts);

        for (String alertBolt : alertBolts) {
            boltPoliciesMap.put(alertBolt, policyDefinitions);
            for (PolicyDefinition policy : policyDefinitions) {
                String policyName = policy.getName();
                alertSpec.addBoltPolicy(alertBolt, policyName);

                for (Publishment publish : publishments) {
                    if (!publish.getPolicyIds().contains(policyName)) {
                        continue;
                    }

                    List<String> streamIds = new ArrayList<>();
                    // add the publish to the bolt
                    if (publish.getStreamIds() == null || publish.getStreamIds().size() <= 0) {
                        streamIds.add(Publishment.STREAM_NAME_DEFAULT);
                    } else {
                        streamIds.addAll(publish.getStreamIds());
                    }
                    for (String streamId : streamIds) {
                        alertSpec.addPublishPartition(streamId, policyName, publish.getName(), publish.getPartitionColumns());
                    }
                }
            }
        }
        alertSpec.setBoltPoliciesMap(boltPoliciesMap);
        return alertSpec;
    }

    public RouterSpec generateRouterSpec() {
        List<StreamWorkSlotQueue> queues = new ArrayList<>();
        List<WorkSlot> workingSlots = new LinkedList<>();
        List<String> alertBolts = generateAlertBolt(numOfAlertBolts);
        workingSlots.addAll(alertBolts.stream().map(alertBolt -> new WorkSlot(Constants.TOPOLOGY_ID, alertBolt)).collect(Collectors.toList()));
        StreamWorkSlotQueue streamWorkSlotQueue = new StreamWorkSlotQueue(new StreamGroup(), false, new HashMap<>(), workingSlots);
        queues.add(streamWorkSlotQueue);
        RouterSpec routerSpec = new RouterSpec(Constants.TOPOLOGY_ID);
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

    public SpoutSpec generateSpout() {
        // streamId -> StreamDefinition
        Map<String, StreamDefinition> streamSchemaMap = new HashMap<>();
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
        for (Kafka2TupleMetadata eachKafka2TupleMetadata : kafka2TupleMetadata) {
            String topic = eachKafka2TupleMetadata.getTopic();
            datasourceTupleMetadataMap.put(eachKafka2TupleMetadata.getName(), eachKafka2TupleMetadata);
            kafka2TupleMetadataMap.put(topic, eachKafka2TupleMetadata);
            tuple2StreamMetadataMap.put(topic, eachKafka2TupleMetadata.getCodec());
        }
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

    private void addGroupingStrategy(Map<String, List<StreamRepartitionMetadata>> streamsMap, String stream,
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