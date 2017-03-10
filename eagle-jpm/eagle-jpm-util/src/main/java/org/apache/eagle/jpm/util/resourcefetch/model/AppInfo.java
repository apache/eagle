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
package org.apache.eagle.jpm.util.resourcefetch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AppInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String user;
    private String name;
    private String queue;
    private String state;
    private String finalStatus;
    private double progress;
    private String trackingUI;
    private String trackingUrl;
    private String diagnostics;
    private String clusterId;
    private String applicationType;
    private long startedTime;
    private long finishedTime;
    private long elapsedTime;
    private String amContainerLogs;
    private String amHostHttpAddress;
    private int allocatedMB;
    private int allocatedVCores;
    private int runningContainers;
    // for HDP 2.7
    private double queueUsagePercentage;
    private double clusterUsagePercentage;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getFinalStatus() {
        return finalStatus;
    }

    public void setFinalStatus(String finalStatus) {
        this.finalStatus = finalStatus;
    }

    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
    }

    public String getTrackingUI() {
        return trackingUI;
    }

    public void setTrackingUI(String trackingUI) {
        this.trackingUI = trackingUI;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public void setApplicationType(String applicationType) {
        this.applicationType = applicationType;
    }

    public long getStartedTime() {
        return startedTime;
    }

    public void setStartedTime(long startedTime) {
        this.startedTime = startedTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getAmContainerLogs() {
        return amContainerLogs;
    }

    public void setAmContainerLogs(String amContainerLogs) {
        this.amContainerLogs = amContainerLogs;
    }

    public String getAmHostHttpAddress() {
        return amHostHttpAddress;
    }

    public void setAmHostHttpAddress(String amHostHttpAddress) {
        this.amHostHttpAddress = amHostHttpAddress;
    }

    public int getAllocatedMB() {
        return allocatedMB;
    }

    public void setAllocatedMB(int allocatedMB) {
        this.allocatedMB = allocatedMB;
    }

    public int getAllocatedVCores() {
        return allocatedVCores;
    }

    public void setAllocatedVCores(int allocatedVCores) {
        this.allocatedVCores = allocatedVCores;
    }

    public int getRunningContainers() {
        return runningContainers;
    }

    public void setRunningContainers(int runningContainers) {
        this.runningContainers = runningContainers;
    }

    public double getQueueUsagePercentage() {
        return queueUsagePercentage;
    }

    public void setQueueUsagePercentage(double queueUsagePercentage) {
        this.queueUsagePercentage = queueUsagePercentage;
    }

    public double getClusterUsagePercentage() {
        return clusterUsagePercentage;
    }

    public void setClusterUsagePercentage(double clusterUsagePercentage) {
        this.clusterUsagePercentage = clusterUsagePercentage;
    }

    @Override
    public String toString() {
        return "AppInfo{"
                + "id='" + id + '\''
                + ", user='" + user + '\''
                + ", name='" + name + '\''
                + ", queue='" + queue + '\''
                + ", state='" + state + '\''
                + ", finalStatus='" + finalStatus + '\''
                + ", progress=" + progress
                + ", trackingUI='" + trackingUI + '\''
                + ", trackingUrl='" + trackingUrl + '\''
                + ", diagnostics='" + diagnostics + '\''
                + ", clusterId='" + clusterId + '\''
                + ", applicationType='" + applicationType + '\''
                + ", startedTime=" + startedTime
                + ", finishedTime=" + finishedTime
                + ", elapsedTime=" + elapsedTime
                + ", amContainerLogs='" + amContainerLogs + '\''
                + ", amHostHttpAddress='" + amHostHttpAddress + '\''
                + ", allocatedMB=" + allocatedMB
                + ", allocatedVCores=" + allocatedVCores
                + ", runningContainers=" + runningContainers
                + '}';
    }
}
