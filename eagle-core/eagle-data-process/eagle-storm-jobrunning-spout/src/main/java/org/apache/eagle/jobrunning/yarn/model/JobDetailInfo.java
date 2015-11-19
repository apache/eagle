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
package org.apache.eagle.jobrunning.yarn.model;

import org.apache.eagle.jobrunning.counter.JobCounters;

public class JobDetailInfo {

	private long startTime;
	private long finishTime;
	private long elapsedTime;
	private String id;
	private String name;
	private String user;
	private String state;
	private int mapsTotal;
	private int mapsCompleted;
	private int reducesTotal;
	private int reducesCompleted;
	private double mapProgress;
	private double reduceProgress;
	private int mapsPending;
	private int mapsRunning;
	private int reducesPending;
	private int reducesRunning;
	private boolean uberized;
	private String diagnostics;
	private int newReduceAttempts;
	private int runningReduceAttempts;
	private int failedReduceAttempts;
	private int killedReduceAttempts;
	private int successfulReduceAttempts;
	private int newMapAttempts;
	private int runningMapAttempts;
	private int failedMapAttempts;
	private int killedMapAttempts;
	private int successfulMapAttempts;
	private String queue;
	private org.apache.eagle.jobrunning.counter.JobCounters jobcounter;
	
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}
	public long getElapsedTime() {
		return elapsedTime;
	}
	public void setElapsedTime(long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public int getMapsTotal() {
		return mapsTotal;
	}
	public void setMapsTotal(int mapsTotal) {
		this.mapsTotal = mapsTotal;
	}
	public int getMapsCompleted() {
		return mapsCompleted;
	}
	public void setMapsCompleted(int mapsCompleted) {
		this.mapsCompleted = mapsCompleted;
	}
	public int getReducesTotal() {
		return reducesTotal;
	}
	public void setReducesTotal(int reducesTotal) {
		this.reducesTotal = reducesTotal;
	}
	public int getReducesCompleted() {
		return reducesCompleted;
	}
	public void setReducesCompleted(int reducesCompleted) {
		this.reducesCompleted = reducesCompleted;
	}
	public double getMapProgress() {
		return mapProgress;
	}
	public void setMapProgress(double mapProgress) {
		this.mapProgress = mapProgress;
	}
	public double getReduceProgress() {
		return reduceProgress;
	}
	public void setReduceProgress(double reduceProgress) {
		this.reduceProgress = reduceProgress;
	}
	public int getMapsPending() {
		return mapsPending;
	}
	public void setMapsPending(int mapsPending) {
		this.mapsPending = mapsPending;
	}
	public int getMapsRunning() {
		return mapsRunning;
	}
	public void setMapsRunning(int mapsRunning) {
		this.mapsRunning = mapsRunning;
	}
	public int getReducesPending() {
		return reducesPending;
	}
	public void setReducesPending(int reducesPending) {
		this.reducesPending = reducesPending;
	}
	public int getReducesRunning() {
		return reducesRunning;
	}
	public void setReducesRunning(int reducesRunning) {
		this.reducesRunning = reducesRunning;
	}
	public boolean isUberized() {
		return uberized;
	}
	public void setUberized(boolean uberized) {
		this.uberized = uberized;
	}
	public String getDiagnostics() {
		return diagnostics;
	}
	public void setDiagnostics(String diagnostics) {
		this.diagnostics = diagnostics;
	}
	public int getNewReduceAttempts() {
		return newReduceAttempts;
	}
	public void setNewReduceAttempts(int newReduceAttempts) {
		this.newReduceAttempts = newReduceAttempts;
	}
	public int getRunningReduceAttempts() {
		return runningReduceAttempts;
	}
	public void setRunningReduceAttempts(int runningReduceAttempts) {
		this.runningReduceAttempts = runningReduceAttempts;
	}
	public int getFailedReduceAttempts() {
		return failedReduceAttempts;
	}
	public void setFailedReduceAttempts(int failedReduceAttempts) {
		this.failedReduceAttempts = failedReduceAttempts;
	}
	public int getKilledReduceAttempts() {
		return killedReduceAttempts;
	}
	public void setKilledReduceAttempts(int killedReduceAttempts) {
		this.killedReduceAttempts = killedReduceAttempts;
	}
	public int getSuccessfulReduceAttempts() {
		return successfulReduceAttempts;
	}
	public void setSuccessfulReduceAttempts(int successfulReduceAttempts) {
		this.successfulReduceAttempts = successfulReduceAttempts;
	}
	public int getNewMapAttempts() {
		return newMapAttempts;
	}
	public void setNewMapAttempts(int newMapAttempts) {
		this.newMapAttempts = newMapAttempts;
	}
	public int getRunningMapAttempts() {
		return runningMapAttempts;
	}
	public void setRunningMapAttempts(int runningMapAttempts) {
		this.runningMapAttempts = runningMapAttempts;
	}
	public int getFailedMapAttempts() {
		return failedMapAttempts;
	}
	public void setFailedMapAttempts(int failedMapAttempts) {
		this.failedMapAttempts = failedMapAttempts;
	}
	public int getKilledMapAttempts() {
		return killedMapAttempts;
	}
	public void setKilledMapAttempts(int killedMapAttempts) {
		this.killedMapAttempts = killedMapAttempts;
	}
	public int getSuccessfulMapAttempts() {
		return successfulMapAttempts;
	}
	public void setSuccessfulMapAttempts(int successfulMapAttempts) {
		this.successfulMapAttempts = successfulMapAttempts;
	}
	public String getQueue() {
		return queue;
	}
	public void setQueue(String queue) {
		this.queue = queue;
	}
	public JobCounters getJobcounter() {
		return jobcounter;
	}
	public void setJobcounter(JobCounters jobcounter) {
		this.jobcounter = jobcounter;
	}
	
	
}
