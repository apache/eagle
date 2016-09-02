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

import static org.apache.eagle.alert.coordinator.CoordinatorConstants.CONFIG_ITEM_BOLT_LOAD_UPBOUND;
import static org.apache.eagle.alert.coordinator.CoordinatorConstants.CONFIG_ITEM_COORDINATOR;
import static org.apache.eagle.alert.coordinator.CoordinatorConstants.POLICIES_PER_BOLT;
import static org.apache.eagle.alert.coordinator.CoordinatorConstants.POLICY_DEFAULT_PARALLELISM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.IPolicyScheduler;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.ScheduleOption;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.coordinator.provider.InMemScheduleConext;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A simple greedy assigner. <br/>
 * A greedy assigner simply loop the policies, find the most suitable topology
 * to locate the policy first, then assign the topics to corresponding
 * spouts/group-by bolts.
 * 
 * <br/>
 * For each given policy, the greedy steps are
 * <ul>
 * <li>1. Find the same topology that already serve the policy without exceed the load</li>
 * <li>2. Find the topology that already take the source traffic without exceed the load</li>
 * <li>3. Find the topology that available to place source topic without exceed the load</li>
 * <li>4. Create a new topology and locate the policy</li>
 * <li>Route table generated after all policies assigned</li>
 * <ul>
 * <br/>
 * 
 * @since Mar 24, 2016
 *
 */
public class GreedyPolicyScheduler implements IPolicyScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(GreedyPolicyScheduler.class);

    private int policiesPerBolt;
    private int policyDefaultParallelism;
    private int initialQueueSize;
    private double boltLoadUpbound;

    // copied context for scheduling
    private IScheduleContext context;

    private TopologyMgmtService mgmtService;

    private ScheduleState state;

    public GreedyPolicyScheduler() {
        Config config = ConfigFactory.load().getConfig(CONFIG_ITEM_COORDINATOR);
        policiesPerBolt = config.getInt(POLICIES_PER_BOLT);
        policyDefaultParallelism = config.getInt(POLICY_DEFAULT_PARALLELISM);
        initialQueueSize = policyDefaultParallelism;
        boltLoadUpbound = config.getDouble(CONFIG_ITEM_BOLT_LOAD_UPBOUND);
    }

    public synchronized ScheduleState schedule(ScheduleOption option) {
        // FIXME: never re-assign right now: sticky mode
        // TODO: how to identify the over-heat nodes? not yet done #Scale of policies
        // Answer: Use configured policiesPerBolt and configured bolt load up-bound
        // FIXME: Here could be strategy to define the topology priorities
        List<WorkItem> workSets = findWorkingSets();
        /**
         * <pre>
         * <ul>
         * <li>how to support take multiple "dumped" topology that consuming the same input as one available sets?</li>
         * Answer: spout spec generated after policy assigned
         * <li>How to define the input traffic partition?</li>
         * Answer: input traffic might not be supported right now.
         * <li>How to support traffic partition between topology?</li> 
         * Answer: two possible place: a global route table will be generated, those target not in current topology tuples will be dropped. This make the partition for tuple to alert
         * <li>How to support add topology on demand by evaluate the available topology bandwidth(need topology level load)?</li>
         * Answer: Use configured topology load up-bound, when topology load is available, will adopt
         * <ul>
         * <pre>
         */
        List<ScheduleResult> results = new ArrayList<ScheduleResult>();
        Map<String, PolicyAssignment> newAssignments = new HashMap<String, PolicyAssignment>();
        for (WorkItem item : workSets) {
            ScheduleResult r = schedulePolicy(item, newAssignments);
            results.add(r);
        }

        state = generateMonitorMetadata(workSets, newAssignments);
        if (LOG.isDebugEnabled()) {
            LOG.debug("calculated schedule state: {}", JsonUtils.writeValueAsString(state));
        }
        return state;
    }

    private List<WorkItem> findWorkingSets() {
        // find the unassigned definition
        List<WorkItem> workSets = new LinkedList<WorkItem>();
        for (PolicyDefinition def : context.getPolicies().values()) {
            int expectParal = def.getParallelismHint();
            if (expectParal == 0) {
                expectParal = policyDefaultParallelism;
            }
            // how to handle expand of an policy in a smooth transition manner
            // TODO policy fix
            PolicyAssignment assignment = context.getPolicyAssignments().get(def.getName());
            if (assignment != null) {
                LOG.info("policy {} already allocated", def.getName());
                continue;
            }

            WorkItem item = new WorkItem(def, expectParal);
            workSets.add(item);
        }
        LOG.info("work set calculation: {}", workSets);
        return workSets;
    }

    private ScheduleState generateMonitorMetadata(List<WorkItem> expandworkSets,
            Map<String, PolicyAssignment> newAssignments) {
        MonitorMetadataGenerator generator = new MonitorMetadataGenerator(context);
        return generator.generate(expandworkSets);
    }

    private void placePolicy(PolicyDefinition def, AlertBoltUsage alertBoltUsage, Topology targetTopology,
            TopologyUsage usage) {
        String policyName = def.getName();

        // topology usage update
        alertBoltUsage.addPolicies(def);

        // update alert policy
        usage.getPolicies().add(policyName);

        // update source topics
        updateDataSource(usage, def);
        
        // update group-by
        updateGrouping(usage, def);
    }

    private void updateGrouping(TopologyUsage usage, PolicyDefinition def) {
        // groupByMeta is removed since groupspec generate doesnt need it now. 
//        List<StreamPartition> policyPartitionSpec = def.getPartitionSpec();
//        Map<String, List<StreamPartition>> groupByMeta = usage.getGroupByMeta();
//        for (StreamPartition par : policyPartitionSpec) {
//            List<StreamPartition> partitions = groupByMeta.get(par.getStreamId());
//            if (partitions == null) {
//                partitions = new ArrayList<StreamPartition>();
//                // de-dup of the partition on the list?
//                groupByMeta.put(par.getStreamId(), partitions);
//            }
//            if (!partitions.contains(par)) {
//                partitions.add(par);
//            }
//        }
    }

    private void updateDataSource(TopologyUsage usage, PolicyDefinition def) {
        List<String> datasources = findDatasource(def);
        usage.getDataSources().addAll(datasources);
    }

    private List<String> findDatasource(PolicyDefinition def) {
        List<String> result = new ArrayList<String>();

        List<String> inputStreams = def.getInputStreams();
        Map<String, StreamDefinition> schemaMaps = context.getStreamSchemas();
        for (String is : inputStreams) {
            StreamDefinition ss = schemaMaps.get(is);
            result.add(ss.getDataSource());
        }
        return result;
    }

    /**
     * For each given policy, the greedy steps are
     * <ul>
     * <li>1. Find the same topology that already serve the policy</li>
     * <li>2. Find the topology that already take the source traffic</li>
     * <li>3. Find the topology that available to place source topic</li>
     * <li>4. Create a new topology and locate the policy</li>
     * <li>Route table generated after all policies assigned</li>
     * <ul>
     * <br/>
     * 
     * @param newAssignments
     */
    private ScheduleResult schedulePolicy(WorkItem item, Map<String, PolicyAssignment> newAssignments) {
        LOG.info(" schedule for {}", item );

        String policyName = item.def.getName();
        StreamGroup policyStreamPartition = new StreamGroup();
        if (item.def.getPartitionSpec().isEmpty()) {
            LOG.error(" policy {} partition spec is empty! ", policyName);
            ScheduleResult result = new ScheduleResult();
            result.policyName = policyName;
            result.code = 400;
            result.message = "policy doesn't have partition spec";
            return result;
        }
        policyStreamPartition.addStreamPartitions(item.def.getPartitionSpec());

        MonitoredStream targetdStream = context.getMonitoredStreams().get(policyStreamPartition);
        if (targetdStream == null) {
            targetdStream = new MonitoredStream(policyStreamPartition);
            context.getMonitoredStreams().put(policyStreamPartition, targetdStream);
        }

        ScheduleResult result = new ScheduleResult();
        result.policyName = policyName;

        StreamWorkSlotQueue queue = findWorkSlotQueue(targetdStream, item.def);
        if (queue == null) {
            result.code = 400;
            result.message = String.format("unable to allocate work queue resource for policy %s !", policyName);
        } else {
            placePolicyToQueue(item.def, queue, newAssignments);
            result.code = 200;
            result.message = "OK";
        }

        LOG.info(" schedule result : {}", result);
        return result;
    }

    private void placePolicyToQueue(PolicyDefinition def, StreamWorkSlotQueue queue,
            Map<String, PolicyAssignment> newAssignments) {
        for (WorkSlot slot : queue.getWorkingSlots()) {
            Topology targetTopology = context.getTopologies().get(slot.getTopologyName());
            TopologyUsage usage = context.getTopologyUsages().get(slot.getTopologyName());
            AlertBoltUsage alertBoltUsage = usage.getAlertBoltUsage(slot.getBoltId());
            placePolicy(def, alertBoltUsage, targetTopology, usage);
        }
//        queue.placePolicy(def);
        PolicyAssignment assignment = new PolicyAssignment(def.getName(), queue.getQueueId());
        context.getPolicyAssignments().put(def.getName(), assignment);
        newAssignments.put(def.getName(), assignment);
    }

    private StreamWorkSlotQueue findWorkSlotQueue(MonitoredStream targetdStream, PolicyDefinition def) {
        StreamWorkSlotQueue targetQueue = null;
        for (StreamWorkSlotQueue queue : targetdStream.getQueues()) {
            if (isQueueAvailable(queue, def)) {
                targetQueue = queue;
                break;
            }
        }

        if (targetQueue == null) {
            WorkQueueBuilder builder = new WorkQueueBuilder(context, mgmtService);
            // TODO : get the properties from policy definiton
            targetQueue = builder.createQueue(targetdStream, false, getQueueSize(def.getParallelismHint()),
                    new HashMap<String, Object>());
        }
        return targetQueue;
    }

    /**
     * Some strategy to generate correct size in Startegy of queue builder
     * 
     * @param hint
     * @return
     */
    private int getQueueSize(int hint) {
    	if (hint == 0) {
    		// some policies require single bolt to execute
    		return 1;
    	}
        return initialQueueSize * ((hint + initialQueueSize - 1) / initialQueueSize);
    }

    private boolean isQueueAvailable(StreamWorkSlotQueue queue, PolicyDefinition def) {
        if (queue.getQueueSize() < def.getParallelismHint()) {
            return false;
        }

        for (WorkSlot slot : queue.getWorkingSlots()) {
            TopologyUsage u = context.getTopologyUsages().get(slot.getTopologyName());
            AlertBoltUsage usage = u.getAlertBoltUsage(slot.getBoltId());
            if (!isBoltAvailable(usage, def)) {
                return false;
            }
        }
        return true;
    }

    private boolean isBoltAvailable(AlertBoltUsage boltUsage, PolicyDefinition def) {
        // overload or over policy # or already contains
        if (boltUsage == null || boltUsage.getLoad() > boltLoadUpbound
                || boltUsage.getPolicies().size() > policiesPerBolt || boltUsage.getPolicies().contains(def.getName())) {
            return false;
        }
        return true;
    }

    public void init(IScheduleContext context, TopologyMgmtService mgmtService) {
        this.context = new InMemScheduleConext(context);
        this.mgmtService = mgmtService;
    }
    
    public IScheduleContext getContext() {
        return context;
    }

    public ScheduleState getState() {
        return state;
    }

}
