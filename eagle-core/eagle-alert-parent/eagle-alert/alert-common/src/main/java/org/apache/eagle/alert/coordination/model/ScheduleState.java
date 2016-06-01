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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

/**
 * A global wise of schedule status <br/>
 * <br/>
 * TODO/FIXME: The persistence simply deserial ScheduleState to Json. One
 * concern is that this string might become too big for store. <br/>
 * <br/>
 * The solution is in metadata resource, have specs/monitoredStreams/policy
 * assignments stored in different table/collections with tage version.
 * 
 * 
 * @since Apr 26, 2016
 *
 */
public class ScheduleState {

    // ScheduleSpec
    private Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();
    private Map<String, AlertBoltSpec> alertSpecs = new HashMap<String, AlertBoltSpec>();
    private Map<String, RouterSpec> groupSpecs = new HashMap<String, RouterSpec>();
    private Map<String, PublishSpec> publishSpecs = new HashMap<String, PublishSpec>();

    // ScheduleSnapshot
    private List<VersionedPolicyDefinition> policySnapshots = new ArrayList<VersionedPolicyDefinition>();
    private List<VersionedStreamDefinition> streamSnapshots = new ArrayList<VersionedStreamDefinition>();

    // ScheduleResult
    private List<MonitoredStream> monitoredStreams = new ArrayList<MonitoredStream>();
    private List<PolicyAssignment> assignments = new ArrayList<PolicyAssignment>();

    private String version;
    // FIXME : should be date, can not make it simple in mongo..
    private String generateTime;
    private int code = 200;
    private String message = "OK";

    public ScheduleState() {
    }

    public ScheduleState(String version, 
            Map<String, SpoutSpec> topoSpoutSpecsMap,
            Map<String, RouterSpec> groupSpecsMap, 
            Map<String, AlertBoltSpec> alertSpecsMap,
            Map<String, PublishSpec> pubMap, 
            Collection<PolicyAssignment> assignments,
            Collection<MonitoredStream> monitoredStreams, 
            Collection<PolicyDefinition> definitions,
            Collection<StreamDefinition> streams) {
        this.spoutSpecs = topoSpoutSpecsMap;
        this.groupSpecs = groupSpecsMap;
        this.alertSpecs = alertSpecsMap;
        this.publishSpecs = pubMap;
        this.version = version;
        this.generateTime = String.valueOf(new Date().getTime());
        this.assignments = new ArrayList<PolicyAssignment>(assignments);
        this.monitoredStreams = new ArrayList<MonitoredStream>(monitoredStreams);
        this.policySnapshots = new ArrayList<VersionedPolicyDefinition>();
        this.streamSnapshots = new ArrayList<VersionedStreamDefinition>();

        for (SpoutSpec ss : this.spoutSpecs.values()) {
            ss.setVersion(version);
        }

        for (RouterSpec ss : this.groupSpecs.values()) {
            ss.setVersion(version);
        }

        for (AlertBoltSpec ss : this.alertSpecs.values()) {
            ss.setVersion(version);
        }

        for (PublishSpec ps : this.publishSpecs.values()) {
            ps.setVersion(version);
        }

        for (MonitoredStream ms : this.monitoredStreams) {
            ms.setVersion(version);
        }
        for (PolicyAssignment ps : this.assignments) {
            ps.setVersion(version);
        }
        for (PolicyDefinition def : definitions) {
            this.policySnapshots.add(new VersionedPolicyDefinition(version, def));
        }
        for (StreamDefinition sd :streams) {
            this.streamSnapshots.add(new VersionedStreamDefinition(version, sd));
        }
    }

    public Map<String, SpoutSpec> getSpoutSpecs() {
        return spoutSpecs;
    }

    public void setSpoutSpecs(Map<String, SpoutSpec> spoutSpecs) {
        this.spoutSpecs = spoutSpecs;
    }

    public Map<String, AlertBoltSpec> getAlertSpecs() {
        return alertSpecs;
    }

    public void setAlertSpecs(Map<String, AlertBoltSpec> alertSpecs) {
        this.alertSpecs = alertSpecs;
    }

    public Map<String, RouterSpec> getGroupSpecs() {
        return groupSpecs;
    }

    public void setGroupSpecs(Map<String, RouterSpec> groupSpecs) {
        this.groupSpecs = groupSpecs;
    }

    public Map<String, PublishSpec> getPublishSpecs() {
        return publishSpecs;
    }

    public void setPublishSpecs(Map<String, PublishSpec> publishSpecs) {
        this.publishSpecs = publishSpecs;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getGenerateTime() {
        return generateTime;
    }

    public void setGenerateTime(String generateTime) {
        this.generateTime = generateTime;
    }

    public List<MonitoredStream> getMonitoredStreams() {
        return monitoredStreams;
    }

    public List<PolicyAssignment> getAssignments() {
        return assignments;
    }

    public List<VersionedPolicyDefinition> getPolicySnapshots() {
        return policySnapshots;
    }

    public void setPolicySnapshots(List<VersionedPolicyDefinition> policySnapshots) {
        this.policySnapshots = policySnapshots;
    }

    public void setMonitoredStreams(List<MonitoredStream> monitoredStreams) {
        this.monitoredStreams = monitoredStreams;
    }

    public void setAssignments(List<PolicyAssignment> assignments) {
        this.assignments = assignments;
    }

    public List<VersionedStreamDefinition> getStreamSnapshots() {
        return streamSnapshots;
    }

    public void setStreamSnapshots(List<VersionedStreamDefinition> streamSnapshots) {
        this.streamSnapshots = streamSnapshots;
    }

}