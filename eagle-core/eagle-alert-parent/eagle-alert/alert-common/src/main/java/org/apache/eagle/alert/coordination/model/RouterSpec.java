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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RouterSpec implements Serializable {
    private static final long serialVersionUID = 4222310571582403820L;
    private String version;
    private String topologyName;

    private List<StreamRouterSpec> routerSpecs;

    public RouterSpec() {
        routerSpecs = new ArrayList<StreamRouterSpec>();
    }

    public RouterSpec(String topoName) {
        this();
        this.topologyName = topoName;
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

    @JsonIgnore
    public void addRouterSpec(StreamRouterSpec routerSpec) {
        routerSpecs.add(routerSpec);
    }

    public List<StreamRouterSpec> getRouterSpecs() {
        return routerSpecs;
    }

    public void setRouterSpecs(List<StreamRouterSpec> routerSpecs) {
        this.routerSpecs = routerSpecs;
    }

    public Map<StreamPartition, List<StreamRouterSpec>> makeSRS() {
        Map<StreamPartition, List<StreamRouterSpec>> newSRS = new HashMap<>();
        this.getRouterSpecs().forEach(t -> {
            if (!newSRS.containsKey(t.getPartition())) {
                newSRS.put(t.getPartition(), new ArrayList<>());
            }
            newSRS.get(t.getPartition()).add(t);
        });
        return newSRS;
    }

    public Map<StreamPartition, StreamSortSpec> makeSSS() {
        Map<StreamPartition, StreamSortSpec> newSSS = new HashMap<>();
        this.getRouterSpecs().forEach(t -> {
            if (t.getPartition().getSortSpec() != null) {
                newSSS.put(t.getPartition(), t.getPartition().getSortSpec());
            }
        });
        return newSSS;
    }

    @Override
    public String toString() {
        return String.format("version:%s-topo:%s, boltSpec:%s", version, topologyName, routerSpecs);
    }
}
