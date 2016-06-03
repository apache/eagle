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

package org.apache.eagle.jpm.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.apache.eagle.jpm.util.Constants;

@Table("eglesprk_apps")
@ColumnFamily("f")
@Prefix("sprkapp")
@Service(Constants.SPARK_APP_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site","sprkAppId", "sprkAppAttemptId", "sprkAppName", "normSprkAppName","user", "queue"})
@Partition({"site"})
public class SparkApp extends TaggedLogAPIEntity{

    @Column("a")
    private long  startTime;
    @Column("b")
    private long endTime;
    @Column("c")
    private String yarnState;
    @Column("d")
    private String yarnStatus;
    @Column("e")
    private JobConfig config;
    @Column("f")
    private int numJobs;
    @Column("g")
    private int totalStages;
    @Column("h")
    private int skippedStages;
    @Column("i")
    private int failedStages;
    @Column("j")
    private int totalTasks;
    @Column("k")
    private int skippedTasks;
    @Column("l")
    private int failedTasks;
    @Column("m")
    private int executors;
    @Column("n")
    private long inputBytes;
    @Column("o")
    private long inputRecords;
    @Column("p")
    private long outputBytes;
    @Column("q")
    private long outputRecords;
    @Column("r")
    private long shuffleReadBytes;
    @Column("s")
    private long shuffleReadRecords;
    @Column("t")
    private long shuffleWriteBytes;
    @Column("u")
    private long shuffleWriteRecords;
    @Column("v")
    private long executorDeserializeTime;
    @Column("w")
    private long executorRunTime;
    @Column("x")
    private long resultSize;
    @Column("y")
    private long jvmGcTime;
    @Column("z")
    private long resultSerializationTime;
    @Column("ab")
    private long memoryBytesSpilled;
    @Column("ac")
    private long diskBytesSpilled;
    @Column("ad")
    private long execMemoryBytes;
    @Column("ae")
    private long driveMemoryBytes;
    @Column("af")
    private int completeTasks;
    @Column("ag")
    private long totalExecutorTime;
    @Column("ah")
    private long executorMemoryOverhead;
    @Column("ai")
    private long driverMemoryOverhead;
    @Column("aj")
    private int executorCores;
    @Column("ak")
    private int driverCores;

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getYarnState() {
        return yarnState;
    }

    public String getYarnStatus() {
        return yarnStatus;
    }

    public int getNumJobs() {
        return numJobs;
    }

    public int getTotalStages() {
        return totalStages;
    }

    public int getSkippedStages() {
        return skippedStages;
    }

    public int getFailedStages() {
        return failedStages;
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public int getSkippedTasks() {
        return skippedTasks;
    }

    public int getFailedTasks() {
        return failedTasks;
    }

    public int getExecutors() {
        return executors;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public long getOutputRecords() {
        return outputRecords;
    }

    public long getShuffleReadBytes() {
        return shuffleReadBytes;
    }

    public long getShuffleReadRecords() {
        return shuffleReadRecords;
    }

    public long getShuffleWriteBytes() {
        return shuffleWriteBytes;
    }

    public long getShuffleWriteRecords() {
        return shuffleWriteRecords;
    }

    public long getExecutorDeserializeTime() {
        return executorDeserializeTime;
    }

    public long getExecutorRunTime() {
        return executorRunTime;
    }

    public long getResultSize() {
        return resultSize;
    }

    public long getJvmGcTime() {
        return jvmGcTime;
    }

    public long getResultSerializationTime() {
        return resultSerializationTime;
    }

    public long getMemoryBytesSpilled() {
        return memoryBytesSpilled;
    }

    public long getDiskBytesSpilled() {
        return diskBytesSpilled;
    }

    public long getExecMemoryBytes() {
        return execMemoryBytes;
    }

    public long getDriveMemoryBytes() {
        return driveMemoryBytes;
    }

    public int getCompleteTasks(){ return completeTasks;}

    public JobConfig getConfig() {
        return config;
    }
    public void setStartTime(long startTime) {
        this.startTime = startTime;
        valueChanged("startTime");
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
        valueChanged("endTime");
    }

    public void setYarnState(String yarnState) {
        this.yarnState = yarnState;
        valueChanged("yarnState");
    }

    public void setYarnStatus(String yarnStatus) {
        this.yarnStatus = yarnStatus;
        valueChanged("yarnStatus");
    }

    public void setConfig(JobConfig config) {
        this.config = config;
        valueChanged("config");
    }

    public void setNumJobs(int numJobs) {
        this.numJobs = numJobs;
        valueChanged("numJobs");
    }

    public void setTotalStages(int totalStages) {
        this.totalStages = totalStages;
        valueChanged("totalStages");
    }

    public void setSkippedStages(int skippedStages) {
        this.skippedStages = skippedStages;
        valueChanged("skippedStages");
    }

    public void setFailedStages(int failedStages) {
        this.failedStages = failedStages;
        valueChanged("failedStages");
    }

    public void setTotalTasks(int totalTasks) {
        this.totalTasks = totalTasks;
        valueChanged("totalTasks");
    }

    public void setSkippedTasks(int skippedTasks) {
        this.skippedTasks = skippedTasks;
        valueChanged("skippedTasks");
    }

    public void setFailedTasks(int failedTasks) {
        this.failedTasks = failedTasks;
        valueChanged("failedTasks");
    }

    public void setExecutors(int executors) {
        this.executors = executors;
        valueChanged("executors");
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
        valueChanged("inputBytes");
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
        valueChanged("inputRecords");
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
        valueChanged("outputBytes");
    }

    public void setOutputRecords(long outputRecords) {
        this.outputRecords = outputRecords;
        valueChanged("outputRecords");
    }

    public void setShuffleReadBytes(long shuffleReadRemoteBytes) {
        this.shuffleReadBytes = shuffleReadRemoteBytes;
        valueChanged("shuffleReadBytes");
    }

    public void setShuffleReadRecords(long shuffleReadRecords) {
        this.shuffleReadRecords = shuffleReadRecords;
        valueChanged("shuffleReadRecords");
    }

    public void setShuffleWriteBytes(long shuffleWriteBytes) {
        this.shuffleWriteBytes = shuffleWriteBytes;
        valueChanged("shuffleWriteBytes");
    }

    public void setShuffleWriteRecords(long shuffleWriteRecords) {
        this.shuffleWriteRecords = shuffleWriteRecords;
        valueChanged("shuffleWriteRecords");
    }

    public void setExecutorDeserializeTime(long executorDeserializeTime) {
        this.executorDeserializeTime = executorDeserializeTime;
        valueChanged("executorDeserializeTime");
    }

    public void setExecutorRunTime(long executorRunTime) {
        this.executorRunTime = executorRunTime;
        valueChanged("executorRunTime");
    }

    public void setResultSize(long resultSize) {
        this.resultSize = resultSize;
        valueChanged("resultSize");
    }

    public void setJvmGcTime(long jvmGcTime) {
        this.jvmGcTime = jvmGcTime;
        valueChanged("jvmGcTime");
    }

    public void setResultSerializationTime(long resultSerializationTime) {
        this.resultSerializationTime = resultSerializationTime;
        valueChanged("resultSerializationTime");
    }

    public void setMemoryBytesSpilled(long memoryBytesSpilled) {
        this.memoryBytesSpilled = memoryBytesSpilled;
        valueChanged("memoryBytesSpilled");
    }

    public void setDiskBytesSpilled(long diskBytesSpilled) {
        this.diskBytesSpilled = diskBytesSpilled;
        valueChanged("diskBytesSpilled");
    }

    public void setExecMemoryBytes(long execMemoryBytes) {
        this.execMemoryBytes = execMemoryBytes;
        valueChanged("execMemoryBytes");
    }

    public void setDriveMemoryBytes(long driveMemoryBytes) {
        this.driveMemoryBytes = driveMemoryBytes;
        valueChanged("driveMemoryBytes");
    }

    public void setCompleteTasks(int completeTasks){
        this.completeTasks = completeTasks;
        valueChanged("completeTasks");
    }

    public long getTotalExecutorTime() {
        return totalExecutorTime;
    }

    public void setTotalExecutorTime(long totalExecutorTime) {
        this.totalExecutorTime = totalExecutorTime;
        valueChanged("totalExecutorTime");
    }

    public long getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public void setExecutorMemoryOverhead(long executorMemoryOverhead) {
        this.executorMemoryOverhead = executorMemoryOverhead;
        valueChanged("executorMemoryOverhead");
    }

    public long getDriverMemoryOverhead() {
        return driverMemoryOverhead;
    }

    public void setDriverMemoryOverhead(long driverMemoryOverhead) {
        this.driverMemoryOverhead = driverMemoryOverhead;
        valueChanged("driverMemoryOverhead");
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
        valueChanged("executorCores");
    }

    public int getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(int driverCores) {
        this.driverCores = driverCores;
        valueChanged("driverCores");
    }
}
