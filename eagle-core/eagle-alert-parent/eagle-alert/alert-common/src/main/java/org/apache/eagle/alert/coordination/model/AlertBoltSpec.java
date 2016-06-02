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
package org.apache.eagle.alert.coordination.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * The alert specification for topology bolts.
 * 
 * @since Apr 29, 2016
 */
public class AlertBoltSpec {
    private String version;
    private String topologyName;

    // mapping from boltId to list of PolicyDefinitions
    @JsonIgnore
    private Map<String, List<PolicyDefinition>> boltPoliciesMap = new HashMap<String, List<PolicyDefinition>>();

    // mapping from boltId to list of PolicyDefinition's Ids
    private Map<String, List<String>> boltPolicyIdsMap = new HashMap<String, List<String>>();

    public AlertBoltSpec() {
    }

    public AlertBoltSpec(String topo) {
        this.topologyName = topo;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

//    public List<PolicyDefinition> getBoltPolicy(String boltId) {
//        return boltPoliciesMap.get(boltId);
//    }
//
//    public void addBoltPolicy(String boltId, PolicyDefinition pd) {
//        if (boltPoliciesMap.containsKey(boltId)) {
//            boltPoliciesMap.get(boltId).add(pd);
//        } else {
//            List<PolicyDefinition> list = new ArrayList<PolicyDefinition>();
//            boltPoliciesMap.put(boltId, list);
//            list.add(pd);
//        }
//    }

    public void addBoltPolicy(String boltId, String policyName) {
        if (boltPolicyIdsMap.containsKey(boltId)) {
            boltPolicyIdsMap.get(boltId).add(policyName);
        } else {
            List<String> list = new ArrayList<String>();
            boltPolicyIdsMap.put(boltId, list);
            list.add(policyName);
        }
    }

    @JsonIgnore
    public Map<String, List<PolicyDefinition>> getBoltPoliciesMap() {
        return boltPoliciesMap;
    }

    @JsonIgnore
    public void setBoltPoliciesMap(Map<String, List<PolicyDefinition>> boltPoliciesMap) {
        this.boltPoliciesMap = boltPoliciesMap;
    }

    public Map<String, List<String>> getBoltPolicyIdsMap() {
        return boltPolicyIdsMap;
    }

    public void setBoltPolicyIdsMap(Map<String, List<String>> boltPolicyIdsMap) {
        this.boltPolicyIdsMap = boltPolicyIdsMap;
    }

    @Override
    public String toString() {
        return String.format("version:%s-topo:%s, boltPolicyIdsMap %s", version, topologyName, boltPolicyIdsMap);
    }

}