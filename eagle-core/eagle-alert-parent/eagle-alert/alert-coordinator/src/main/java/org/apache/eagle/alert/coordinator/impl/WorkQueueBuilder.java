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

import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.impl.strategies.IWorkSlotStrategy;
import org.apache.eagle.alert.coordinator.impl.strategies.SameTopologySlotStrategy;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since Apr 27, 2016.
 */
public class WorkQueueBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(WorkQueueBuilder.class);

    private final IScheduleContext context;
    private final TopologyMgmtService mgmtService;

    public WorkQueueBuilder(IScheduleContext context, TopologyMgmtService mgmtService) {
        this.context = context;
        this.mgmtService = mgmtService;
    }

    public StreamWorkSlotQueue createQueue(MonitoredStream stream, boolean isDedicated, int size,
                                           Map<String, Object> properties) {
        // FIXME: make extensible and configurable
        IWorkSlotStrategy strategy = new SameTopologySlotStrategy(context, stream.getStreamGroup(), mgmtService);
        List<WorkSlot> slots = strategy.reserveWorkSlots(size, isDedicated, properties);
        if (slots.size() < size) {
            LOG.error("allocate stream work queue failed, required size");
            return null;
        }
        StreamWorkSlotQueue queue = new StreamWorkSlotQueue(stream.getStreamGroup(), isDedicated, properties,
            slots);
        calculateGroupIndexAndCount(queue);
        assignQueueSlots(stream, queue);// build reverse reference
        stream.addQueues(queue);

        return queue;
    }

    private void assignQueueSlots(MonitoredStream stream, StreamWorkSlotQueue queue) {
        for (WorkSlot slot : queue.getWorkingSlots()) {
            TopologyUsage u = context.getTopologyUsages().get(slot.getTopologyName());
            AlertBoltUsage boltUsage = u.getAlertBoltUsage(slot.getBoltId());
            boltUsage.addQueue(stream.getStreamGroup(), queue);
            u.addMonitoredStream(stream);
        }
    }

    private void calculateGroupIndexAndCount(StreamWorkSlotQueue queue) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        int total = 0;
        for (WorkSlot slot : queue.getWorkingSlots()) {
            if (result.containsKey(slot.getTopologyName())) {
                continue;
            }
            result.put(slot.getTopologyName(), total);
            total += context.getTopologies().get(slot.getTopologyName()).getNumOfGroupBolt();
        }

        queue.setNumberOfGroupBolts(total);
        queue.setTopoGroupStartIndex(result);
    }

}
