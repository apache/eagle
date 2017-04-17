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

import org.apache.eagle.alert.engine.coordinator.Publishment;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

public class PublishSpec implements Serializable {

    private static final long serialVersionUID = -8393697498219611661L;
    private String topologyName;
    // actually only publish spec for one topology
    private String boltId;
    private String version;

    private List<Publishment> publishments = new ArrayList<Publishment>();

    public PublishSpec() {
    }

    public PublishSpec(String topoName, String boltId) {
        this.topologyName = topoName;
        this.boltId = boltId;
    }

    @JsonIgnore
    public void addPublishment(Publishment p) {
        if (!this.publishments.contains(p)) {
            this.publishments.add(p);
        }
    }

    public String getTopologyName() {
        return topologyName;
    }

    public String getBoltId() {
        return boltId;
    }

    public List<Publishment> getPublishments() {
        return publishments;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public void setBoltId(String boltId) {
        this.boltId = boltId;
    }

    public void setPublishments(List<Publishment> publishments) {
        this.publishments = publishments;
    }

}
