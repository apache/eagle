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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;

/**
 * @since Mar 27, 2016
 *
 */
public class TopologyUsage {
    // topo info
    private String topoName;
    private final Set<String> datasources = new HashSet<String>();
    // usage information
    private final Set<String> policies = new HashSet<String>();
    private final Map<String, AlertBoltUsage> alertUsages = new HashMap<String, AlertBoltUsage>();
    private final Map<String, GroupBoltUsage> groupUsages = new HashMap<String, GroupBoltUsage>();
    private final List<MonitoredStream> monitoredStream = new ArrayList<MonitoredStream>();

    private double load;

    /**
     * This is to be the existing/previous meta-data. <br/>
     * Only one group meta-data for all of the group bolts in this topology.
     */

    public TopologyUsage() {
    }
    
    public TopologyUsage(String name) {
        this.topoName = name;
    }
    
    public String getTopoName() {
        return topoName;
    }

    public void setTopoName(String topoId) {
        this.topoName = topoId;
    }

    public Set<String> getDataSources() {
        return datasources;
    }

    public Set<String> getPolicies() {
        return policies;
    }

    public Map<String, AlertBoltUsage> getAlertUsages() {
        return alertUsages;
    }

    public AlertBoltUsage getAlertBoltUsage(String boltId) {
        return alertUsages.get(boltId);
    }

    public Map<String, GroupBoltUsage> getGroupUsages() {
        return groupUsages;
    }

    public List<MonitoredStream> getMonitoredStream() {
        return monitoredStream;
    }

    public void addMonitoredStream(MonitoredStream par) {
        if (!this.monitoredStream.contains(par)) {
            this.monitoredStream.add(par);
        }
    }

    public double getLoad() {
        return load;
    }

    public void setLoad(double load) {
        this.load = load;
    }

}
