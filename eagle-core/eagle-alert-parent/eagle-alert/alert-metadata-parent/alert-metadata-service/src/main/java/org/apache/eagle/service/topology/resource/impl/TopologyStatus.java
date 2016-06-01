/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.topology.resource.impl;

import org.apache.eagle.alert.coordination.model.internal.Topology;

import java.util.HashMap;
import java.util.Map;

public class TopologyStatus {
    private String name;
    private String id;
    private String state;
    private Topology topology;

    private Map<String, Double> spoutLoad = new HashMap<>();
    private Map<String, Double> boltLoad = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, Double> getSpoutLoad() {
        return spoutLoad;
    }

    public void setSpoutLoad(Map<String, Double> spoutLoad) {
        this.spoutLoad = spoutLoad;
    }

    public Map<String, Double> getBoltLoad() {
        return boltLoad;
    }

    public void setBoltLoad(Map<String, Double> boltLoad) {
        this.boltLoad = boltLoad;
    }

    public Topology getTopology() {
        return topology;
    }

    public void setTopology(Topology topology) {
        this.topology = topology;
    }
}
