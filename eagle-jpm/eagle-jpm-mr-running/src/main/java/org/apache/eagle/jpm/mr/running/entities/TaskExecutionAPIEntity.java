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
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagleMRRunningTasks")
@ColumnFamily("f")
@Prefix("tasks_exec")
@Service(Constants.JPA_RUNNING_TASK_EXECUTION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})

@Tags({"site", "jobId", "JobName", "jobNormalName", "jobType", "taskType", "taskId", "user", "queue"})
public class TaskExecutionAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private long startTime;
    @Column("b")
    private long finishTime;
    @Column("c")
    private long elapsedTime;
    @Column("d")
    private double progress;
    @Column("e")
    private String status;
    @Column("f")
    private String successfulAttempt;
    @Column("g")
    private String statusDesc;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        valueChanged("startTime");
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
        valueChanged("finishTime");
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
        valueChanged("elapsedTime");
    }

    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
        valueChanged("progress");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public String getSuccessfulAttempt() {
        return successfulAttempt;
    }

    public void setSuccessfulAttempt(String successfulAttempt) {
        this.successfulAttempt = successfulAttempt;
        valueChanged("successfulAttempt");
    }

    public String getStatusDesc() {
        return statusDesc;
    }

    public void setStatusDesc(String statusDesc) {
        this.statusDesc = statusDesc;
        valueChanged("statusDesc");
    }
}
