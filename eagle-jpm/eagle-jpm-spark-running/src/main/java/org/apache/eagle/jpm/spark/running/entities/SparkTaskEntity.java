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

package org.apache.eagle.jpm.spark.running.entities;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.apache.eagle.jpm.util.Constants;

@Table("eagleSparkRunningTasks")
@ColumnFamily("f")
@Prefix("sparkTask")
@Service(Constants.RUNNING_SPARK_TASK_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site","sparkAppId", "sparkAppAttemptId", "sparkAppName", "jobId", "jobName", "stageId","stageAttemptId","taskIndex","taskAttemptId","user", "queue"})
@Partition({"site"})
public class SparkTaskEntity extends TaggedLogAPIEntity {
    @Column("a")
    private int taskId;
    @Column("b")
    private long launchTime;
    @Column("c")
    private String executorId;
    @Column("d")
    private String host;
    @Column("e")
    private String taskLocality;
    @Column("f")
    private boolean speculative;
    @Column("g")
    private long executorDeserializeTime;
    @Column("h")
    private long executorRunTime;
    @Column("i")
    private long resultSize;
    @Column("j")
    private long jvmGcTime;
    @Column("k")
    private long resultSerializationTime;
    @Column("l")
    private long memoryBytesSpilled;
    @Column("m")
    private long diskBytesSpilled;
    @Column("n")
    private long inputBytes;
    @Column("o")
    private long inputRecords;
    @Column("p")
    private long outputBytes;
    @Column("q")
    private long outputRecords;
    @Column("r")
    private long shuffleReadRemoteBytes;
    @Column("x")
    private long shuffleReadLocalBytes;
    @Column("s")
    private long shuffleReadRecords;
    @Column("t")
    private long shuffleWriteBytes;
    @Column("u")
    private long shuffleWriteRecords;
    @Column("v")
    private boolean failed;

    public int getTaskId() {
        return taskId;
    }

    public long getLaunchTime() {
        return launchTime;
    }

    public String getExecutorId() {
        return executorId;
    }

    public String getHost() {
        return host;
    }

    public String getTaskLocality() {
        return taskLocality;
    }

    public boolean isSpeculative() {
        return speculative;
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

    public long getShuffleReadRecords() {
        return shuffleReadRecords;
    }

    public long getShuffleWriteBytes() {
        return shuffleWriteBytes;
    }

    public long getShuffleWriteRecords() {
        return shuffleWriteRecords;
    }

    public boolean isFailed() {
        return failed;
    }

    public long getShuffleReadRemoteBytes() {
        return shuffleReadRemoteBytes;
    }

    public long getShuffleReadLocalBytes() {
        return shuffleReadLocalBytes;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
        valueChanged("failed");
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
        valueChanged("taskId");
    }

    public void setLaunchTime(long launchTime) {
        this.launchTime = launchTime;
        valueChanged("launchTime");
    }

    public void setExecutorId(String executorId) {
        this.executorId = executorId;
        valueChanged("executorId");
    }

    public void setHost(String host) {
        this.host = host;
        this.valueChanged("host");
    }

    public void setTaskLocality(String taskLocality) {
        this.taskLocality = taskLocality;
        this.valueChanged("taskLocality");
    }

    public void setSpeculative(boolean speculative) {
        this.speculative = speculative;
        this.valueChanged("speculative");
    }

    public void setExecutorDeserializeTime(long executorDeserializeTime) {
        this.executorDeserializeTime = executorDeserializeTime;
        this.valueChanged("executorDeserializeTime");
    }

    public void setExecutorRunTime(long executorRunTime) {
        this.executorRunTime = executorRunTime;
        this.valueChanged("executorRunTime");
    }

    public void setResultSize(long resultSize) {
        this.resultSize = resultSize;
        this.valueChanged("resultSize");
    }

    public void setJvmGcTime(long jvmGcTime) {
        this.jvmGcTime = jvmGcTime;
        this.valueChanged("jvmGcTime");
    }

    public void setResultSerializationTime(long resultSerializationTime) {
        this.resultSerializationTime = resultSerializationTime;
        this.valueChanged("resultSerializationTime");
    }

    public void setMemoryBytesSpilled(long memoryBytesSpilled) {
        this.memoryBytesSpilled = memoryBytesSpilled;
        this.valueChanged("memoryBytesSpilled");
    }

    public void setDiskBytesSpilled(long diskBytesSpilled) {
        this.diskBytesSpilled = diskBytesSpilled;
        this.valueChanged("diskBytesSpilled");
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
        this.valueChanged("inputBytes");
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
        this.valueChanged("inputRecords");
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
        this.valueChanged("outputBytes");
    }

    public void setOutputRecords(long outputRecords) {
        this.outputRecords = outputRecords;
        this.valueChanged("outputRecords");
    }



    public void setShuffleReadRecords(long shuffleReadRecords) {
        this.shuffleReadRecords = shuffleReadRecords;
        this.valueChanged("shuffleReadRecords");
    }

    public void setShuffleWriteBytes(long shuffleWriteBytes) {
        this.shuffleWriteBytes = shuffleWriteBytes;
        this.valueChanged("shuffleWriteBytes");
    }

    public void setShuffleWriteRecords(long shuffleWriteRecords) {
        this.shuffleWriteRecords = shuffleWriteRecords;
        this.valueChanged("shuffleWriteRecords");
    }

    public void setShuffleReadRemoteBytes(long shuffleReadRemoteBytes) {
        this.shuffleReadRemoteBytes = shuffleReadRemoteBytes;
        this.valueChanged("shuffleReadRemoteBytes");
    }

    public void setShuffleReadLocalBytes(long shuffleReadLocalBytes) {
        this.shuffleReadLocalBytes = shuffleReadLocalBytes;
        this.valueChanged("shuffleReadLocalBytes");
    }
}
