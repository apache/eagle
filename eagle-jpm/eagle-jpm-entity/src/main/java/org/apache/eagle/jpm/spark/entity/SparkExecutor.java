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

package org.apache.eagle.jpm.spark.entity;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@Table("eglesprk_executors")
@ColumnFamily("f")
@Prefix("sprkexcutr")
@Service(Constants.SPARK_EXECUTOR_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site","sprkAppId", "sprkAppAttemptId", "sprkAppName", "normSprkAppName", "executorId","user", "queue"})
@Partition({"site"})
public class SparkExecutor extends TaggedLogAPIEntity {

    @Column("a")
    private String hostPort;
    @Column("b")
    private int rddBlocks;
    @Column("c")
    private long memoryUsed;
    @Column("d")
    private long diskUsed;
    @Column("e")
    private int activeTasks = 0;
    @Column("f")
    private int failedTasks = 0;
    @Column("g")
    private int completedTasks = 0;
    @Column("h")
    private int totalTasks = 0;
    @Column("i")
    private long totalDuration = 0;
    @Column("j")
    private long totalInputBytes = 0;
    @Column("k")
    private long totalShuffleRead = 0;
    @Column("l")
    private long totalShuffleWrite = 0;
    @Column("m")
    private long maxMemory;
    @Column("n")
    private long startTime;
    @Column("o")
    private long endTime = 0;
    @Column("p")
    private long execMemoryBytes;
    @Column("q")
    private int cores;
    @Column("r")
    private long memoryOverhead;

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
        this.valueChanged("hostPort");
    }

    public int getRddBlocks() {
        return rddBlocks;
    }

    public void setRddBlocks(int rddBlocks) {
        this.rddBlocks = rddBlocks;
        this.valueChanged("rddBlocks");
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public void setMemoryUsed(long memoryUsed) {
        this.memoryUsed = memoryUsed;
        this.valueChanged("memoryUsed");
    }

    public long getDiskUsed() {
        return diskUsed;
    }

    public void setDiskUsed(long diskUsed) {
        this.diskUsed = diskUsed;
        this.valueChanged("diskUsed");
    }

    public int getActiveTasks() {
        return activeTasks;
    }

    public void setActiveTasks(int activeTasks) {
        this.activeTasks = activeTasks;
        this.valueChanged("activeTasks");
    }

    public int getFailedTasks() {
        return failedTasks;
    }

    public void setFailedTasks(int failedTasks) {
        this.failedTasks = failedTasks;
        this.valueChanged("failedTasks");
    }

    public int getCompletedTasks() {
        return completedTasks;
    }

    public void setCompletedTasks(int completedTasks) {
        this.completedTasks = completedTasks;
        this.valueChanged("completedTasks");
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void setTotalTasks(int totalTasks) {
        this.totalTasks = totalTasks;
        this.valueChanged("totalTasks");
    }

    public long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
        this.valueChanged("totalDuration");
    }

    public long getTotalInputBytes() {
        return totalInputBytes;
    }

    public void setTotalInputBytes(long totalInputBytes) {
        this.totalInputBytes = totalInputBytes;
        this.valueChanged("totalInputBytes");
    }

    public long getTotalShuffleRead() {
        return totalShuffleRead;
    }

    public void setTotalShuffleRead(long totalShuffleRead) {
        this.totalShuffleRead = totalShuffleRead;
        this.valueChanged("totalShuffleRead");
    }

    public long getTotalShuffleWrite() {
        return totalShuffleWrite;
    }

    public void setTotalShuffleWrite(long totalShuffleWrite) {
        this.totalShuffleWrite = totalShuffleWrite;
        this.valueChanged("totalShuffleWrite");
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
        this.valueChanged("maxMemory");
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
        this.valueChanged("endTime");
    }

    public long getExecMemoryBytes() {
        return execMemoryBytes;
    }

    public void setExecMemoryBytes(long execMemoryBytes) {
        this.execMemoryBytes = execMemoryBytes;
        this.valueChanged("execMemoryBytes");
    }

    public int getCores() {
        return cores;
    }

    public void setCores(int cores) {
        this.cores = cores;
        valueChanged("cores");
    }

    public long getMemoryOverhead() {
        return memoryOverhead;
    }

    public void setMemoryOverhead(long memoryOverhead) {
        this.memoryOverhead = memoryOverhead;
        valueChanged("memoryOverhead");
    }
}
