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

package org.apache.eagle.jpm.mr.historyentity;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa_task")
@ColumnFamily("f")
@Prefix("taexec")
@Service(Constants.JPA_TASK_ATTEMPT_EXECUTION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Indexes({
    @Index(name = "Index_1_jobId", columns = { "jobId" }, unique = false)
    })
public class TaskAttemptExecutionAPIEntity extends JobBaseAPIEntity {
    @Column("a")
    private String taskStatus;
    @Column("b")
    private long startTime;
    @Column("c")
    private long endTime;
    @Column("d")
    private long duration;
    @Column("e")
    private String error;
    @Column("f")
    private JobCounters jobCounters;
    @Column("g")
    private String taskAttemptID;

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
        pcs.firePropertyChange("taskStatus", null, null);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        pcs.firePropertyChange("startTime", null, null);
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
        pcs.firePropertyChange("endTime", null, null);
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
        pcs.firePropertyChange("duration", null, null);
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
        pcs.firePropertyChange("error", null, null);
    }

    public JobCounters getJobCounters() {
        return jobCounters;
    }

    public void setJobCounters(JobCounters jobCounters) {
        this.jobCounters = jobCounters;
        pcs.firePropertyChange("jobCounters", null, null);
    }

    public String getTaskAttemptID() {
        return taskAttemptID;
    }

    public void setTaskAttemptID(String taskAttemptID) {
        this.taskAttemptID = taskAttemptID;
        pcs.firePropertyChange("taskAttemptID", null, null);
    }
}
