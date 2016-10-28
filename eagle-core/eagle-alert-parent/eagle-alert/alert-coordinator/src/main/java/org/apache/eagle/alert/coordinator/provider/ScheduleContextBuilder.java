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
package org.apache.eagle.alert.coordinator.provider;

import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.*;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.GroupBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition.PolicyStatus;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * FIXME: this class focus on correctness, not the efficiency now. There might
 * be problem when metadata size grows too big.
 *
 * @since May 3, 2016
 */
public class ScheduleContextBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleContextBuilder.class);
    private static final String UNIQUE_BOLT_ID = "%s-%s";// toponame-boltname

    private Config config;
    private IMetadataServiceClient client;

    private Map<String, Topology> topologies;
    private Map<String, PolicyAssignment> assignments;
    private Map<String, Kafka2TupleMetadata> kafkaSources;
    private Map<String, PolicyDefinition> policies;
    private Map<String, Publishment> publishments;
    private Map<String, StreamDefinition> streamDefinitions;
    private Map<StreamGroup, MonitoredStream> monitoredStreamMap;
    private Map<String, TopologyUsage> usages;
    private IScheduleContext builtContext;

    public ScheduleContextBuilder(Config config) {
        this.config = config;
        client = new MetadataServiceClientImpl(config);
    }

    public ScheduleContextBuilder(Config config, IMetadataServiceClient client) {
        this.config = config;
        this.client = client;
    }

    /**
     * Built a shcedule context for metadata client service.
     *
     * @return
     */
    public IScheduleContext buildContext() {
        topologies = listToMap(client.listTopologies());
        kafkaSources = listToMap(client.listDataSources());
        // filter out disabled policies
        List<PolicyDefinition> enabledPolicies = client.listPolicies().stream().filter(
            (t) -> t.getPolicyStatus() != PolicyStatus.DISABLED).collect(Collectors.toList());
        policies = listToMap(enabledPolicies);
        publishments = listToMap(client.listPublishment());
        streamDefinitions = listToMap(client.listStreams());
        // generate data sources, policies, publishments for nodata alert
        new NodataMetadataGenerator().execute(config, streamDefinitions, kafkaSources, policies, publishments);

        // TODO: See ScheduleState comments on how to improve the storage
        ScheduleState state = client.getVersionedSpec();

        // detect policy update, remove the policy assignments.
        // definition change : the assignment would NOT change, the runtime will do reload and check
        // stream change : the assignment would NOT change, the runtime will do reload and check
        // data source change : the assignment would NOT change, the runtime will do reload and check
        // parallelism change : the policies' assignment would be dropped when it's bigger than assign queue, and expect
        //      to be assigned in scheduler.
        assignments = listToMap(state == null ? new ArrayList<PolicyAssignment>() : detectAssignmentsChange(state.getAssignments(), state));

        monitoredStreamMap = listToMap(state == null ? new ArrayList<MonitoredStream>() : detectMonitoredStreams(state.getMonitoredStreams()));

        // build based on existing data
        usages = buildTopologyUsage();

        // copy to shedule context now
        builtContext = new InMemScheduleConext(topologies, assignments, kafkaSources, policies, publishments,
            streamDefinitions, monitoredStreamMap, usages);
        return builtContext;
    }

    public IScheduleContext getBuiltContext() {
        return builtContext;
    }

    /**
     * 1.
     * <pre>
     * Check for deprecated policy stream group with its assigned monitored stream groups.
     * If this is unmatched, we think the policy' stream group has been changed, remove the policy assignments
     * If finally, no assignment refer to a given monitored stream, this monitored stream could be removed.
     * Log when every time a remove happens.
     * </pre>
     * 2.
     * <pre>
     * if monitored stream's queue's is on non-existing topology, remove it.
     * </pre>
     *
     * @param monitoredStreams
     * @return
     */
    private List<MonitoredStream> detectMonitoredStreams(List<MonitoredStream> monitoredStreams) {
        List<MonitoredStream> result = new ArrayList<MonitoredStream>(monitoredStreams);

        // clear deprecated streams
        clearMonitoredStreams(monitoredStreams);

        // build queueId-> streamGroup
        Map<String, StreamGroup> queue2StreamGroup = new HashMap<String, StreamGroup>();
        for (MonitoredStream ms : result) {
            for (StreamWorkSlotQueue q : ms.getQueues()) {
                queue2StreamGroup.put(q.getQueueId(), ms.getStreamGroup());
            }
        }

        // decide the assignment delete set
        Set<StreamGroup> usedGroups = new HashSet<StreamGroup>();
        Set<String> toRemove = new HashSet<String>();
        // check if queue is still referenced by policy assignments
        for (PolicyAssignment assignment : assignments.values()) {
            PolicyDefinition def = policies.get(assignment.getPolicyName());
            StreamGroup group = queue2StreamGroup.get(assignment.getQueueId());
            if (group == null || !Objects.equals(group.getStreamPartitions(), def.getPartitionSpec())) {
                LOG.warn(" policy assgiment {} 's policy group {} is different to the monitored stream's partition group {}, "
                        + "this indicates a policy stream partition spec change, the assignment would be removed! ",
                    assignment, def.getPartitionSpec(), group == null ? "'not found'" : group.getStreamPartitions());
                toRemove.add(assignment.getPolicyName());
            } else {
                usedGroups.add(group);
            }
        }

        // remove useless
        assignments.keySet().removeAll(toRemove);
        // remove non-referenced monitored streams
        result.removeIf((t) -> {
            boolean used = usedGroups.contains(t.getStreamGroup());
            if (!used) {
                LOG.warn("monitor stream with stream group {} is not referenced, "
                    + "this monitored stream and its worker queu will be removed", t.getStreamGroup());
            }
            return !used;
        });

        return result;
    }

    private void clearMonitoredStreams(List<MonitoredStream> monitoredStreams) {
        Iterator<MonitoredStream> it = monitoredStreams.iterator();
        while (it.hasNext()) {
            MonitoredStream ms = it.next();
            Iterator<StreamWorkSlotQueue> queueIt = ms.getQueues().iterator();
            // clean queue that underly topology is changed(removed/down)
            while (queueIt.hasNext()) {
                StreamWorkSlotQueue queue = queueIt.next();
                boolean deprecated = false;
                for (WorkSlot ws : queue.getWorkingSlots()) {
                    // check if topology available or bolt available
                    if (!topologies.containsKey(ws.topologyName)
                        || !topologies.get(ws.topologyName).getAlertBoltIds().contains(ws.boltId)) {
                        deprecated = true;
                        break;
                    }
                }
                if (deprecated) {
                    queueIt.remove();
                }
            }

            if (ms.getQueues().isEmpty()) {
                it.remove();
            }
        }
    }

    private List<PolicyAssignment> detectAssignmentsChange(List<PolicyAssignment> list, ScheduleState state) {
        // FIXME: duplciated build map ?
        Map<String, StreamWorkSlotQueue> queueMap = new HashMap<String, StreamWorkSlotQueue>();
        for (MonitoredStream ms : state.getMonitoredStreams()) {
            for (StreamWorkSlotQueue q : ms.getQueues()) {
                queueMap.put(q.getQueueId(), q);
            }
        }

        List<PolicyAssignment> result = new ArrayList<PolicyAssignment>(list);
        Iterator<PolicyAssignment> paIt = result.iterator();
        while (paIt.hasNext()) {
            PolicyAssignment assignment = paIt.next();

            if (!policies.containsKey(assignment.getPolicyName())) {
                LOG.info("Policy assignment {} 's policy not found, this assignment will be removed!", assignment);
                paIt.remove();
            } else {
                StreamWorkSlotQueue queue = queueMap.get(assignment.getQueueId());
                if (queue == null
                    || policies.get(assignment.getPolicyName()).getParallelismHint() != queue.getQueueSize()) {
                    // queue not found or policy has hint not equal to queue (possible a poilcy update)
                    LOG.info("Policy assignment {} 's policy doesnt match queue: {}!", assignment, queue);
                    paIt.remove();
                }
            }
        }
        return result;
    }

    public static <T, K> Map<K, T> listToMap(List<T> collections) {
        Map<K, T> maps = new HashMap<K, T>(collections.size());
        for (T t : collections) {
            maps.put(getKey(t), t);
        }
        return maps;
    }

    /*
     * One drawback, once we add class, this code need to be changed!
     */
    @SuppressWarnings("unchecked")
    private static <T, K> K getKey(T t) {
        if (t instanceof Topology) {
            return (K) ((Topology) t).getName();
        } else if (t instanceof PolicyAssignment) {
            return (K) ((PolicyAssignment) t).getPolicyName();
        } else if (t instanceof Kafka2TupleMetadata) {
            return (K) ((Kafka2TupleMetadata) t).getName();
        } else if (t instanceof PolicyDefinition) {
            return (K) ((PolicyDefinition) t).getName();
        } else if (t instanceof Publishment) {
            return (K) ((Publishment) t).getName();
        } else if (t instanceof StreamDefinition) {
            return (K) ((StreamDefinition) t).getStreamId();
        } else if (t instanceof MonitoredStream) {
            return (K) ((MonitoredStream) t).getStreamGroup();
        }
        throw new RuntimeException("unexpected key class " + t.getClass());
    }

    private Map<String, TopologyUsage> buildTopologyUsage() {
        Map<String, TopologyUsage> usages = new HashMap<String, TopologyUsage>();

        // pre-build data structure for help
        Map<String, Set<MonitoredStream>> topo2MonitorStream = new HashMap<String, Set<MonitoredStream>>();
        Map<String, Set<String>> topo2Policies = new HashMap<String, Set<String>>();
        // simply assume no bolt with the same id
        Map<String, Set<String>> bolt2Policies = new HashMap<String, Set<String>>();
        // simply assume no bolt with the same id
        Map<String, Set<StreamGroup>> bolt2Partition = new HashMap<String, Set<StreamGroup>>();
        // simply assume no bolt with the same id
        Map<String, Set<String>> bolt2QueueIds = new HashMap<String, Set<String>>();
        Map<String, StreamWorkSlotQueue> queueMap = new HashMap<String, StreamWorkSlotQueue>();

        preBuildQueue2TopoMap(topo2MonitorStream, topo2Policies, bolt2Policies, bolt2Partition, bolt2QueueIds, queueMap);

        for (Topology t : topologies.values()) {
            TopologyUsage u = new TopologyUsage(t.getName());
            // add group/bolt usages
            for (String grpBolt : t.getGroupNodeIds()) {
                GroupBoltUsage grpUsage = new GroupBoltUsage(grpBolt);
                u.getGroupUsages().put(grpBolt, grpUsage);
            }
            for (String alertBolt : t.getAlertBoltIds()) {
                String uniqueBoltId = String.format(UNIQUE_BOLT_ID, t.getName(), alertBolt);

                AlertBoltUsage alertUsage = new AlertBoltUsage(alertBolt);
                u.getAlertUsages().put(alertBolt, alertUsage);
                // complete usage
                addBoltUsageInfo(bolt2Policies, bolt2Partition, bolt2QueueIds, uniqueBoltId, alertUsage, queueMap);
            }

            // policy -- policy assignment
            if (topo2Policies.containsKey(u.getTopoName())) {
                u.getPolicies().addAll(topo2Policies.get(u.getTopoName()));
            }

            // data source
            buildTopologyDataSource(u);

            // topology usage monitored stream -- from monitored steams' queue slot item info
            if (topo2MonitorStream.containsKey(u.getTopoName())) {
                u.getMonitoredStream().addAll(topo2MonitorStream.get(u.getTopoName()));
            }

            usages.put(u.getTopoName(), u);
        }

        return usages;
    }

    private void addBoltUsageInfo(Map<String, Set<String>> bolt2Policies,
                                  Map<String, Set<StreamGroup>> bolt2Partition, Map<String, Set<String>> bolt2QueueIds, String uniqueAlertBolt,
                                  AlertBoltUsage alertUsage, Map<String, StreamWorkSlotQueue> queueMap) {
        //
        if (bolt2Policies.containsKey(uniqueAlertBolt)) {
            alertUsage.getPolicies().addAll(bolt2Policies.get(uniqueAlertBolt));
        }
        //
        if (bolt2Partition.containsKey(uniqueAlertBolt)) {
            alertUsage.getPartitions().addAll(bolt2Partition.get(uniqueAlertBolt));
        }
        //
        if (bolt2QueueIds.containsKey(uniqueAlertBolt)) {
            for (String qId : bolt2QueueIds.get(uniqueAlertBolt)) {
                if (queueMap.containsKey(qId)) {
                    alertUsage.getReferQueues().add(queueMap.get(qId));
                } else {
                    LOG.error(" queue id {} not found in queue map !", qId);
                }
            }
        }
    }

    private void buildTopologyDataSource(TopologyUsage u) {
        for (String policyName : u.getPolicies()) {
            PolicyDefinition def = policies.get(policyName);
            if (def != null) {
                u.getDataSources().addAll(findDatasource(def));
            } else {
                LOG.error(" policy not find {}, but reference in topology usage {} !", policyName, u.getTopoName());
            }
        }
    }

    private List<String> findDatasource(PolicyDefinition def) {
        List<String> result = new ArrayList<String>();
        List<String> inputStreams = def.getInputStreams();
        for (String is : inputStreams) {
            StreamDefinition ss = this.streamDefinitions.get(is);
            if (ss == null) {
                LOG.error("policy {} referenced stream definition {} not found in definiton !", def.getName(), is);
            } else {
                result.add(ss.getDataSource());
            }
        }
        return result;
    }

    private void preBuildQueue2TopoMap(
        Map<String, Set<MonitoredStream>> topo2MonitorStream,
        Map<String, Set<String>> topo2Policies,
        Map<String, Set<String>> bolt2Policies,
        Map<String, Set<StreamGroup>> bolt2Partition,
        Map<String, Set<String>> bolt2QueueIds,
        Map<String, StreamWorkSlotQueue> queueMap) {
        // pre-build structure
        // why don't reuse the queue.getPolicies
        Map<String, Set<String>> queue2Policies = new HashMap<String, Set<String>>();
        for (PolicyAssignment pa : assignments.values()) {
            if (!queue2Policies.containsKey(pa.getQueueId())) {
                queue2Policies.put(pa.getQueueId(), new HashSet<String>());
            }
            queue2Policies.get(pa.getQueueId()).add(pa.getPolicyName());
        }

        for (MonitoredStream stream : monitoredStreamMap.values()) {
            for (StreamWorkSlotQueue q : stream.getQueues()) {
                queueMap.put(q.getQueueId(), q);
                Set<String> policiesOnQ = queue2Policies.containsKey(q.getQueueId()) ? queue2Policies.get(q.getQueueId()) : new HashSet<String>();

                for (WorkSlot slot : q.getWorkingSlots()) {
                    // topo2monitoredstream
                    if (!topo2MonitorStream.containsKey(slot.getTopologyName())) {
                        topo2MonitorStream.put(slot.getTopologyName(), new HashSet<MonitoredStream>());
                    }
                    topo2MonitorStream.get(slot.getTopologyName()).add(stream);

                    // topo2policy
                    if (!topo2Policies.containsKey(slot.getTopologyName())) {
                        topo2Policies.put(slot.getTopologyName(), new HashSet<String>());
                    }
                    topo2Policies.get(slot.getTopologyName()).addAll(policiesOnQ);

                    // bolt2Policy
                    if (!bolt2Policies.containsKey(getUniqueBoltId(slot))) {
                        bolt2Policies.put(getUniqueBoltId(slot), new HashSet<String>());
                    }
                    bolt2Policies.get(getUniqueBoltId(slot)).addAll(policiesOnQ);

                    // bolt2Queue
                    if (!bolt2QueueIds.containsKey(getUniqueBoltId(slot))) {
                        bolt2QueueIds.put(getUniqueBoltId(slot), new HashSet<String>());
                    }
                    bolt2QueueIds.get(getUniqueBoltId(slot)).add(q.getQueueId());

                    // bolt2Partition
                    if (!bolt2Partition.containsKey(getUniqueBoltId(slot))) {
                        bolt2Partition.put(getUniqueBoltId(slot), new HashSet<StreamGroup>());
                    }
                    bolt2Partition.get(getUniqueBoltId(slot)).add(stream.getStreamGroup());
                }
            }
        }
    }

    private String getUniqueBoltId(WorkSlot slot) {
        return String.format(UNIQUE_BOLT_ID, slot.getTopologyName(), slot.getBoltId());
    }

}
