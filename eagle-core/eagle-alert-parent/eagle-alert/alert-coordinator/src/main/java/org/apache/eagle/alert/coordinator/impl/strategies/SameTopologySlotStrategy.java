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
package org.apache.eagle.alert.coordinator.impl.strategies;

import static org.apache.eagle.alert.coordinator.CoordinatorConstants.CONFIG_ITEM_TOPOLOGY_LOAD_UPBOUND;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.CoordinatorConstants;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.TopologyMgmtService.TopologyMeta;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * A simple strategy that only find the bolts in the same topology as the
 * required work slots.
 * <p>
 * Invariant:<br/>
 * One slot queue only on the one topology.<br/>
 * One topology doesn't contains two same partition slot queues.
 *
 * @since Apr 27, 2016
 */
public class SameTopologySlotStrategy implements IWorkSlotStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SameTopologySlotStrategy.class);

    private final IScheduleContext context;
    private final StreamGroup partitionGroup;
    private final TopologyMgmtService mgmtService;

    //    private final int numOfPoliciesBoundPerBolt;
    private final double topoLoadUpbound;

    public SameTopologySlotStrategy(IScheduleContext context, StreamGroup streamPartitionGroup,
                                    TopologyMgmtService mgmtService) {
        this.context = context;
        this.partitionGroup = streamPartitionGroup;
        this.mgmtService = mgmtService;

        Config config = ConfigFactory.load().getConfig(CoordinatorConstants.CONFIG_ITEM_COORDINATOR);
//        numOfPoliciesBoundPerBolt = config.getInt(CoordinatorConstants.POLICIES_PER_BOLT);
        topoLoadUpbound = config.getDouble(CONFIG_ITEM_TOPOLOGY_LOAD_UPBOUND);
    }

    /**
     * @param isDedicated - not used yet!
     */
    @Override
    public List<WorkSlot> reserveWorkSlots(int size, boolean isDedicated, Map<String, Object> properties) {
        Iterator<Topology> it = context.getTopologies().values().stream().filter((t) -> t.getNumOfAlertBolt() >= size)
            .iterator();
        // priority strategy first???
        List<WorkSlot> slots = new ArrayList<WorkSlot>();
        while (it.hasNext()) {
            Topology t = it.next();
            if (getQueueOnTopology(size, slots, t)) {
                break;
            }
        }

        if (slots.size() == 0) {
            int supportedSize = mgmtService.getNumberOfAlertBoltsInTopology();
            if (size > supportedSize) {
                LOG.error("can not find available slots for queue, required size {}, supported size {} !", size, supportedSize);
                return Collections.emptyList();
            }
            TopologyMeta topoMeta = mgmtService.creatTopology();
            if (topoMeta == null) {
                LOG.error("can not create topology for given queue requirement, required size {}, requried partition group {} !", size, partitionGroup);
                return Collections.emptyList();
            }

            context.getTopologies().put(topoMeta.topologyId, topoMeta.topology);
            context.getTopologyUsages().put(topoMeta.topologyId, topoMeta.usage);
            boolean placed = getQueueOnTopology(size, slots, topoMeta.topology);
            if (!placed) {
                LOG.error("can not find available slots from new created topology, required size {}. This indicates an error !", size);
            }
        }
        return slots;
    }

    private boolean getQueueOnTopology(int size, List<WorkSlot> slots, Topology t) {
        TopologyUsage u = context.getTopologyUsages().get(t.getName());
        if (!isTopologyAvailable(u)) {
            return false;
        }

        List<String> bolts = new ArrayList<String>();
        for (AlertBoltUsage alertUsage : u.getAlertUsages().values()) {
            if (isBoltAvailable(alertUsage)) {
                bolts.add(alertUsage.getBoltId());
            }

            if (bolts.size() == size) {
                break;
            }
        }

        if (bolts.size() == size) {
            for (String boltId : bolts) {
                WorkSlot slot = new WorkSlot(t.getName(), boltId);
                slots.add(slot);
            }
            return true;
        }
        return false;
    }

    private boolean isTopologyAvailable(TopologyUsage u) {
//        for (MonitoredStream stream : u.getMonitoredStream()) {
//            if (partition.equals(stream.getStreamParitition())) {
//                return false;
//            }
//        }
        if (u == null || u.getLoad() > topoLoadUpbound) {
            return false;
        }

        return true;
    }

    private boolean isBoltAvailable(AlertBoltUsage alertUsage) {
        // FIXME : more detail to compare on queue exclusion check
        if (alertUsage.getQueueSize() > 0) {
            return false;
        }
        // actually it's now 0;
        return true;
//        return alertUsage.getPolicies().size() < numOfPoliciesBoundPerBolt;
    }

}
