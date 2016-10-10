/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.extractor.mr;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class YarnNodeInfo {

    private String rack;
    private String state;
    private String id;
    private String nodeHostName;
    private String nodeHTTPAddress;
    private String lastHealthUpdate;
    private String healthReport;
    private String numContainers;
    private String usedMemoryMB;
    private String availMemoryMB;


    public String getRack() {
        return rack;
    }
    public void setRack(String rack) {
        this.rack = rack;
    }
    public String getState() {
        return state;
    }
    public void setState(String state) {
        this.state = state;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getNodeHostName() {
        return nodeHostName;
    }
    public void setNodeHostName(String nodeHostName) {
        this.nodeHostName = nodeHostName;
    }
    public String getNodeHTTPAddress() {
        return nodeHTTPAddress;
    }
    public void setNodeHTTPAddress(String nodeHTTPAddress) {
        this.nodeHTTPAddress = nodeHTTPAddress;
    }
    public String getLastHealthUpdate() {
        return lastHealthUpdate;
    }
    public void setLastHealthUpdate(String lastHealthUpdate) {
        this.lastHealthUpdate = lastHealthUpdate;
    }
    public String getHealthReport() {
        return healthReport;
    }
    public void setHealthReport(String healthReport) {
        this.healthReport = healthReport;
    }
    public String getNumContainers() {
        return numContainers;
    }
    public void setNumContainers(String numContainers) {
        this.numContainers = numContainers;
    }
    public String getUsedMemoryMB() {
        return usedMemoryMB;
    }
    public void setUsedMemoryMB(String usedMemoryMB) {
        this.usedMemoryMB = usedMemoryMB;
    }
    public String getAvailMemoryMB() {
        return availMemoryMB;
    }
    public void setAvailMemoryMB(String availMemoryMB) {
        this.availMemoryMB = availMemoryMB;
    }

}
