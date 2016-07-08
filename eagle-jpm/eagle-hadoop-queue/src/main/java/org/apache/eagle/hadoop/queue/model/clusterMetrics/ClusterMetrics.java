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

package org.apache.eagle.hadoop.queue.model.clusterMetrics;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterMetrics {
	private int appsSubmitted;
	private int appsCompleted;
	private int appsPending;
	private int appsRunning;
	private int appsFailed;
	private int appsKilled;
	private long reservedMB;
	private long availableMB;
	private long allocatedMB;
	private int containersAllocated;
	private int containersReserved;
	private int containersPending;
	private long totalMB;
	private int totalNodes;
	private int lostNodes;
	private int unhealthyNodes;
	private int decommissionedNodes;
	private int rebootedNodes;
	private int activeNodes;

	public int getAppsSubmitted() {
		return appsSubmitted;
	}

	public void setAppsSubmitted(int appsSubmitted) {
		this.appsSubmitted = appsSubmitted;
	}

	public int getAppsCompleted() {
		return appsCompleted;
	}

	public void setAppsCompleted(int appsCompleted) {
		this.appsCompleted = appsCompleted;
	}

	public int getAppsPending() {
		return appsPending;
	}

	public void setAppsPending(int appsPending) {
		this.appsPending = appsPending;
	}

	public int getAppsRunning() {
		return appsRunning;
	}

	public void setAppsRunning(int appsRunning) {
		this.appsRunning = appsRunning;
	}

	public int getAppsFailed() {
		return appsFailed;
	}

	public void setAppsFailed(int appsFailed) {
		this.appsFailed = appsFailed;
	}

	public int getAppsKilled() {
		return appsKilled;
	}

	public void setAppsKilled(int appsKilled) {
		this.appsKilled = appsKilled;
	}

	public long getReservedMB() {
		return reservedMB;
	}

	public void setReservedMB(long reservedMB) {
		this.reservedMB = reservedMB;
	}

	public long getAvailableMB() {
		return availableMB;
	}

	public void setAvailableMB(long availableMB) {
		this.availableMB = availableMB;
	}

	public long getAllocatedMB() {
		return allocatedMB;
	}

	public void setAllocatedMB(long allocatedMB) {
		this.allocatedMB = allocatedMB;
	}

	public int getContainersAllocated() {
		return containersAllocated;
	}

	public void setContainersAllocated(int containersAllocated) {
		this.containersAllocated = containersAllocated;
	}

	public int getContainersReserved() {
		return containersReserved;
	}

	public void setContainersReserved(int containersReserved) {
		this.containersReserved = containersReserved;
	}

	public int getContainersPending() {
		return containersPending;
	}

	public void setContainersPending(int containersPending) {
		this.containersPending = containersPending;
	}

	public long getTotalMB() {
		return totalMB;
	}

	public void setTotalMB(long totalMB) {
		this.totalMB = totalMB;
	}

	public int getTotalNodes() {
		return totalNodes;
	}

	public void setTotalNodes(int totalNodes) {
		this.totalNodes = totalNodes;
	}

	public int getLostNodes() {
		return lostNodes;
	}

	public void setLostNodes(int lostNodes) {
		this.lostNodes = lostNodes;
	}

	public int getUnhealthyNodes() {
		return unhealthyNodes;
	}

	public void setUnhealthyNodes(int unhealthyNodes) {
		this.unhealthyNodes = unhealthyNodes;
	}

	public int getDecommissionedNodes() {
		return decommissionedNodes;
	}

	public void setDecommissionedNodes(int decommissionedNodes) {
		this.decommissionedNodes = decommissionedNodes;
	}

	public int getRebootedNodes() {
		return rebootedNodes;
	}

	public void setRebootedNodes(int rebootedNodes) {
		this.rebootedNodes = rebootedNodes;
	}

	public int getActiveNodes() {
		return activeNodes;
	}

	public void setActiveNodes(int activeNodes) {
		this.activeNodes = activeNodes;
	}
}
