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
package org.apache.eagle.alert.coordinator.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

/**
 * @since Mar 28, 2016
 *
 */
public class AlertBoltUsage {

    private String boltId;
    private List<String> policies = new ArrayList<String>();
    // the stream partitions group that scheduled for this given alert bolt
    private List<StreamGroup> partitions = new ArrayList<StreamGroup>();
    // the slot queue that scheduled for this given aler bolt
    private List<StreamWorkSlotQueue> referQueues = new ArrayList<StreamWorkSlotQueue>();
    private double load;

    public AlertBoltUsage(String anid) {
        this.boltId = anid;
    }

    public String getBoltId() {
        return boltId;
    }

    public void setBoltId(String boltId) {
        this.boltId = boltId;
    }

    public List<String> getPolicies() {
        return policies;
    }

    public void addPolicies(PolicyDefinition pd) {
        policies.add(pd.getName());
        // add first partition
//        for (StreamPartition par : pd.getPartitionSpec()) {
//            partitions.add(par);
//        }
    }

    public double getLoad() {
        return load;
    }

    public void setLoad(double load) {
        this.load = load;
    }

    public List<StreamGroup> getPartitions() {
        return partitions;
    }

    public List<StreamWorkSlotQueue> getReferQueues() {
        return referQueues;
    }
    
    public int getQueueSize() {
        return referQueues.size();
    }

    public void addQueue(StreamGroup streamPartition, StreamWorkSlotQueue queue) {
        this.referQueues.add(queue);
        this.partitions.add(streamPartition);
    }

    public void removeQueue(StreamWorkSlotQueue queue) {
        this.referQueues.remove(queue);
    }

}
