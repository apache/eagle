/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.coordinator.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.StreamRepartitionStrategy;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since Apr 26, 2016
 * Given current policy placement, figure out monitor metadata
 * <p>
 * TODO: refactor to eliminate the duplicate of stupid if-notInMap-then-create....
 * FIXME: too many duplicated code logic : check null; add list to map; add to list..
 */
public class MonitorMetadataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorMetadataGenerator.class);

    private IScheduleContext context;

    public MonitorMetadataGenerator(IScheduleContext context) {
        this.context = context;
    }

    public ScheduleState generate(List<WorkItem> expandworkSets) {
        // topologyId -> SpoutSpec
        Map<String, SpoutSpec> topoSpoutSpecsMap = generateSpoutMonitorMetadata();

        // grp-by meta spec(sort & grp)
        Map<String, RouterSpec> groupSpecsMap = generateGroupbyMonitorMetadata();

        // alert bolt spec
        Map<String, AlertBoltSpec> alertSpecsMap = generateAlertMonitorMetadata();

        Map<String, PublishSpec> publishSpecsMap = generatePublishMetadata();

        String uniqueVersion = generateVersion();
        ScheduleState status = new ScheduleState(uniqueVersion,
            topoSpoutSpecsMap,
            groupSpecsMap,
            alertSpecsMap,
            publishSpecsMap,
            context.getPolicyAssignments().values(),
            context.getMonitoredStreams().values(),
            context.getPolicies().values(),
            context.getStreamSchemas().values());
        return status;
    }

    private Map<String, PublishSpec> generatePublishMetadata() {
        Map<String, PublishSpec> pubSpecs = new HashMap<String, PublishSpec>();
        // prebuild policy to publishment map
        Map<String, List<Publishment>> policyToPub = new HashMap<String, List<Publishment>>();
        for (Publishment pub : context.getPublishments().values()) {
            for (String policyId : pub.getPolicyIds()) {
                List<Publishment> policyPubs = policyToPub.get(policyId);
                if (policyPubs == null) {
                    policyPubs = new ArrayList<>();
                    policyToPub.put(policyId, policyPubs);
                }
                policyPubs.add(pub);
            }
        }

        // build per topology
        for (TopologyUsage u : context.getTopologyUsages().values()) {
            PublishSpec pubSpec = pubSpecs.get(u.getTopoName());
            if (pubSpec == null) {
                pubSpec = new PublishSpec(u.getTopoName(), context.getTopologies().get(u.getTopoName()).getPubBoltId());
                pubSpecs.put(u.getTopoName(), pubSpec);
            }

            for (String p : u.getPolicies()) {
                PolicyDefinition definition = context.getPolicies().get(p);
                if (definition == null) {
                    continue;
                }
                if (policyToPub.containsKey(p)) {
                    for (Publishment pub : policyToPub.get(p)) {
                        pubSpec.addPublishment(pub);
                    }
                }
            }
        }
        return pubSpecs;
    }

    /**
     * FIXME: add auto-increment version number?
     */
    private String generateVersion() {
        return "spec_version_" + System.currentTimeMillis();
    }

    private Map<String, AlertBoltSpec> generateAlertMonitorMetadata() {
        Map<String, AlertBoltSpec> alertSpecs = new HashMap<String, AlertBoltSpec>();
        for (TopologyUsage u : context.getTopologyUsages().values()) {
            AlertBoltSpec alertSpec = alertSpecs.get(u.getTopoName());
            if (alertSpec == null) {
                alertSpec = new AlertBoltSpec(u.getTopoName());
                alertSpecs.put(u.getTopoName(), alertSpec);
            }
            for (AlertBoltUsage boltUsage : u.getAlertUsages().values()) {
                for (String policyName : boltUsage.getPolicies()) {
                    PolicyDefinition definition = context.getPolicies().get(policyName);
                    alertSpec.addBoltPolicy(boltUsage.getBoltId(), definition.getName());
                }
            }
        }
        return alertSpecs;
    }

    private Map<String, RouterSpec> generateGroupbyMonitorMetadata() {
        Map<String, RouterSpec> groupSpecsMap = new HashMap<String, RouterSpec>();
        for (TopologyUsage u : context.getTopologyUsages().values()) {
            RouterSpec spec = groupSpecsMap.get(u.getTopoName());
            if (spec == null) {
                spec = new RouterSpec(u.getTopoName());
                groupSpecsMap.put(u.getTopoName(), spec);
            }

            for (MonitoredStream ms : u.getMonitoredStream()) {
                // mutiple stream on the same policy group : for correlation group case:
                for (StreamPartition partiton : ms.getStreamGroup().getStreamPartitions()) {
                    StreamRouterSpec routeSpec = new StreamRouterSpec();
                    routeSpec.setPartition(partiton);
                    routeSpec.setStreamId(partiton.getStreamId());

                    for (StreamWorkSlotQueue sq : ms.getQueues()) {
                        PolicyWorkerQueue queue = new PolicyWorkerQueue();
                        queue.setWorkers(sq.getWorkingSlots());
                        queue.setPartition(partiton);
                        routeSpec.addQueue(queue);
                    }

                    spec.addRouterSpec(routeSpec);
                }
            }
        }

        return groupSpecsMap;
    }

    private Map<String, SpoutSpec> generateSpoutMonitorMetadata() {
        Map<String, StreamWorkSlotQueue> queueMap = buildQueueMap();

        Map<String, SpoutSpec> topoSpoutSpecsMap = new HashMap<String, SpoutSpec>();
        // streamName -> StreamDefinition
        Map<String, StreamDefinition> streamSchemaMap = context.getStreamSchemas();
        Map<String, Kafka2TupleMetadata> datasourcesMap = context.getDataSourceMetadata();
        for (TopologyUsage usage : context.getTopologyUsages().values()) {
            Topology topo = context.getTopologies().get(usage.getTopoName());

            // based on data source schemas
            // generate topic -> Kafka2TupleMetadata
            // generate topic -> Tuple2StreamMetadata (actually the schema selector)
            Map<String, Kafka2TupleMetadata> dss = new HashMap<String, Kafka2TupleMetadata>();
            Map<String, Tuple2StreamMetadata> tss = new HashMap<String, Tuple2StreamMetadata>();
            for (String dataSourceId : usage.getDataSources()) {
                Kafka2TupleMetadata ds = datasourcesMap.get(dataSourceId);
                dss.put(ds.getTopic(), ds);
                tss.put(ds.getTopic(), ds.getCodec());
            }

            // generate topicId -> StreamRepartitionMetadata
            Map<String, List<StreamRepartitionMetadata>> streamsMap = new HashMap<String, List<StreamRepartitionMetadata>>();
            for (String policyName : usage.getPolicies()) {
                PolicyDefinition def = context.getPolicies().get(policyName);

                PolicyAssignment assignment = context.getPolicyAssignments().get(policyName);
                if (assignment == null) {
                    LOG.error(" can not find assignment for policy {} ! ", policyName);
                    continue;
                }

                for (StreamPartition policyStreamPartition : def.getPartitionSpec()) {
                    String stream = policyStreamPartition.getStreamId();
                    StreamDefinition schema = streamSchemaMap.get(stream);
                    String topic = datasourcesMap.get(schema.getDataSource()).getTopic();

                    // add stream name to tuple metadata
                    if (tss.containsKey(topic)) {
                        Tuple2StreamMetadata tupleMetadata = tss.get(topic);
                        tupleMetadata.getActiveStreamNames().add(stream);
                    }

                    // grouping strategy
                    StreamRepartitionStrategy gs = new StreamRepartitionStrategy();
                    gs.partition = policyStreamPartition;
                    gs.numTotalParticipatingRouterBolts = queueMap.get(assignment.getQueueId()).getNumberOfGroupBolts();
                    gs.startSequence = queueMap.get(assignment.getQueueId()).getTopologyGroupStartIndex(topo.getName());
                    gs.totalTargetBoltIds = new ArrayList<String>(topo.getGroupNodeIds());

                    // add to map
                    addGroupingStrategy(streamsMap, stream, schema, topic, schema.getDataSource(), gs);
                }
            }

            SpoutSpec spoutSpec = new SpoutSpec(topo.getName(), streamsMap, tss, dss);
            topoSpoutSpecsMap.put(topo.getName(), spoutSpec);
        }
        return topoSpoutSpecsMap;
    }

    /**
     * Work queue not a root level object, thus we need to build a map from
     * MonitoredStream for later quick lookup
     *
     * @return
     */
    private Map<String, StreamWorkSlotQueue> buildQueueMap() {
        Map<String, StreamWorkSlotQueue> queueMap = new HashMap<String, StreamWorkSlotQueue>();
        for (MonitoredStream ms : context.getMonitoredStreams().values()) {
            for (StreamWorkSlotQueue queue : ms.getQueues()) {
                queueMap.put(queue.getQueueId(), queue);
            }
        }
        return queueMap;
    }

    private void addGroupingStrategy(Map<String, List<StreamRepartitionMetadata>> streamsMap, String stream,
                                     StreamDefinition schema, String topicName, String datasourceName, StreamRepartitionStrategy gs) {
        List<StreamRepartitionMetadata> dsStreamMeta;
        if (streamsMap.containsKey(topicName)) {
            dsStreamMeta = streamsMap.get(topicName);
        } else {
            dsStreamMeta = new ArrayList<StreamRepartitionMetadata>();
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
            targetSm = new StreamRepartitionMetadata(datasourceName, schema.getStreamId());
            dsStreamMeta.add(targetSm);
        }
        if (!targetSm.groupingStrategies.contains(gs)) {
            targetSm.addGroupStrategy(gs);
        }
    }

}
