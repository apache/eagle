/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.running.entities;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagleMRRunningJobs")
@ColumnFamily("f")
@Prefix("jobs")
@Service(Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Indexes({
        @Index(name="Index_1_jobId", columns = { "jobID" }, unique = true),
        @Index(name="Index_2_normJobName", columns = { "normJobName" }, unique = false)
})
@Tags({"site", "jobId", "jobName", "jobNormalName", "jobType", "user", "queue"})
public class JobExecutionAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private long startTime;
    @Column("b")
    private long endTime;
    @Column("c")
    private long elapsedTime;
    @Column("d")
    private String status;
    @Column("e")
    private int mapsTotal;
    @Column("f")
    private int mapsCompleted;
    @Column("g")
    private int reducesTotal;
    @Column("h")
    private int reducesCompleted;
    @Column("i")
    private double mapProgress;
    @Column("j")
    private double reduceProgress;
    @Column("k")
    private int mapsPending;
    @Column("l")
    private int mapsRunning;
    @Column("m")
    private int reducesPending;
    @Column("n")
    private int reducesRunning;
    @Column("o")
    private int newReduceAttempts;
    @Column("p")
    private int runningReduceAttempts;
    @Column("q")
    private int failedReduceAttempts;
    @Column("r")
    private int killedReduceAttempts;
    @Column("s")
    private int successfulReduceAttempts;
    @Column("t")
    private int newMapAttempts;
    @Column("u")
    private int runningMapAttempts;
    @Column("v")
    private int failedMapAttempts;
    @Column("w")
    private int killedMapAttempts;
    @Column("x")
    private int successfulMapAttempts;
    @Column("y")
    private AppInfo appInfo;
    @Column("z")
    private JobCounters jobCounters;
    @Column("aa")
    private JobConfig jobConfig;

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    public JobCounters getJobCounters() {
        return jobCounters;
    }

    public void setJobCounters(JobCounters jobCounters) {
        this.jobCounters = jobCounters;
        valueChanged("jobCounters");
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        valueChanged("startTime");
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
        valueChanged("endTime");
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
        valueChanged("elapsedTime");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public int getMapsTotal() {
        return mapsTotal;
    }

    public void setMapsTotal(int mapsTotal) {
        this.mapsTotal = mapsTotal;
        valueChanged("mapsTotal");
    }

    public int getMapsCompleted() {
        return mapsCompleted;
    }

    public void setMapsCompleted(int mapsCompleted) {
        this.mapsCompleted = mapsCompleted;
        valueChanged("mapsCompleted");
    }

    public int getReducesTotal() {
        return reducesTotal;
    }

    public void setReducesTotal(int reducesTotal) {
        this.reducesTotal = reducesTotal;
        valueChanged("reducesTotal");
    }

    public int getReducesCompleted() {
        return reducesCompleted;
    }

    public void setReducesCompleted(int reducesCompleted) {
        this.reducesCompleted = reducesCompleted;
        valueChanged("reducesCompleted");
    }

    public double getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(double mapProgress) {
        this.mapProgress = mapProgress;
        valueChanged("mapProgress");
    }

    public double getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(double reduceProgress) {
        this.reduceProgress = reduceProgress;
        valueChanged("reduceProgress");
    }

    public int getMapsPending() {
        return mapsPending;
    }

    public void setMapsPending(int mapsPending) {
        this.mapsPending = mapsPending;
        valueChanged("mapsPending");
    }

    public int getMapsRunning() {
        return mapsRunning;
    }

    public void setMapsRunning(int mapsRunning) {
        this.mapsRunning = mapsRunning;
        valueChanged("mapsRunning");
    }

    public int getReducesPending() {
        return reducesPending;
    }

    public void setReducesPending(int reducesPending) {
        this.reducesPending = reducesPending;
        valueChanged("reducesPending");
    }

    public int getReducesRunning() {
        return reducesRunning;
    }

    public void setReducesRunning(int reducesRunning) {
        this.reducesRunning = reducesRunning;
        valueChanged("reducesRunning");
    }

    public int getNewReduceAttempts() {
        return newReduceAttempts;
    }

    public void setNewReduceAttempts(int newReduceAttempts) {
        this.newReduceAttempts = newReduceAttempts;
        valueChanged("newReduceAttempts");
    }

    public int getRunningReduceAttempts() {
        return runningReduceAttempts;
    }

    public void setRunningReduceAttempts(int runningReduceAttempts) {
        this.runningReduceAttempts = runningReduceAttempts;
        valueChanged("runningReduceAttempts");
    }

    public int getFailedReduceAttempts() {
        return failedReduceAttempts;
    }

    public void setFailedReduceAttempts(int failedReduceAttempts) {
        this.failedReduceAttempts = failedReduceAttempts;
        valueChanged("failedReduceAttempts");
    }

    public int getKilledReduceAttempts() {
        return killedReduceAttempts;
    }

    public void setKilledReduceAttempts(int killedReduceAttempts) {
        this.killedReduceAttempts = killedReduceAttempts;
        valueChanged("killedReduceAttempts");
    }

    public int getSuccessfulReduceAttempts() {
        return successfulReduceAttempts;
    }

    public void setSuccessfulReduceAttempts(int successfulReduceAttempts) {
        this.successfulReduceAttempts = successfulReduceAttempts;
        valueChanged("successfulReduceAttempts");
    }

    public int getNewMapAttempts() {
        return newMapAttempts;
    }

    public void setNewMapAttempts(int newMapAttempts) {
        this.newMapAttempts = newMapAttempts;
        valueChanged("newMapAttempts");
    }

    public int getRunningMapAttempts() {
        return runningMapAttempts;
    }

    public void setRunningMapAttempts(int runningMapAttempts) {
        this.runningMapAttempts = runningMapAttempts;
        valueChanged("runningMapAttempts");
    }

    public int getFailedMapAttempts() {
        return failedMapAttempts;
    }

    public void setFailedMapAttempts(int failedMapAttempts) {
        this.failedMapAttempts = failedMapAttempts;
        valueChanged("failedMapAttempts");
    }

    public int getKilledMapAttempts() {
        return killedMapAttempts;
    }

    public void setKilledMapAttempts(int killedMapAttempts) {
        this.killedMapAttempts = killedMapAttempts;
        valueChanged("killedMapAttempts");
    }

    public int getSuccessfulMapAttempts() {
        return successfulMapAttempts;
    }

    public void setSuccessfulMapAttempts(int successfulMapAttempts) {
        this.successfulMapAttempts = successfulMapAttempts;
        valueChanged("successfulMapAttempts");
    }

    public AppInfo getAppInfo() {
        return appInfo;
    }

    public void setAppInfo(AppInfo appInfo) {
        this.appInfo = appInfo;
        valueChanged("successfulMapAttempts");
    }
}
