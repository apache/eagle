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
package org.apache.eagle.alert.coordination.model.internal;

import org.apache.eagle.alert.coordination.model.WorkSlot;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A work queue for given one monitored stream.
 *
 * <p>Analog to storm's "tasks for given bolt".
 *
 * @since Apr 27, 2016
 */
public class StreamWorkSlotQueue {
    private String queueId;

    private final List<WorkSlot> workingSlots = new LinkedList<WorkSlot>();
    private boolean dedicated;
    // some dedicated option, like dedicated userId/tenantId/policyId.
    private Map<String, Object> dedicateOption;

    private int numberOfGroupBolts;
    private Map<String, Integer> topoGroupStartIndex = new HashMap<String, Integer>();

    public StreamWorkSlotQueue() {
    }

    public StreamWorkSlotQueue(StreamGroup par, boolean isDedicated, Map<String, Object> options,
                               List<WorkSlot> slots) {
        this.queueId = par.getStreamId() + System.currentTimeMillis();// simply generate a queue
        this.dedicated = isDedicated;
        dedicateOption = new HashMap<String, Object>();
        dedicateOption.putAll(options);
        this.workingSlots.addAll(slots);
    }

    public Map<String, Object> getDedicateOption() {
        return dedicateOption;
    }

    public void setDedicateOption(Map<String, Object> dedicateOption) {
        this.dedicateOption = dedicateOption;
    }

    public List<WorkSlot> getWorkingSlots() {
        return workingSlots;
    }

    public boolean isDedicated() {
        return dedicated;
    }

    public void setDedicated(boolean dedicated) {
        this.dedicated = dedicated;
    }

    @JsonIgnore
    public int getQueueSize() {
        return workingSlots.size();
    }

    //    @org.codehaus.jackson.annotate.JsonIgnore
    //    @JsonIgnore
    //    public void placePolicy(PolicyDefinition pd) {
    //        policies.add(pd.getName());
    //    }

    public int getNumberOfGroupBolts() {
        return numberOfGroupBolts;
    }

    public void setNumberOfGroupBolts(int numberOfGroupBolts) {
        this.numberOfGroupBolts = numberOfGroupBolts;
    }

    public Map<String, Integer> getTopoGroupStartIndex() {
        return topoGroupStartIndex;
    }

    public void setTopoGroupStartIndex(Map<String, Integer> topoGroupStartIndex) {
        this.topoGroupStartIndex = topoGroupStartIndex;
    }

    @JsonIgnore
    public int getTopologyGroupStartIndex(String topo) {
        if (topoGroupStartIndex.containsKey(topo)) {
            return this.topoGroupStartIndex.get(topo);
        }
        return -1;
    }

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

}
