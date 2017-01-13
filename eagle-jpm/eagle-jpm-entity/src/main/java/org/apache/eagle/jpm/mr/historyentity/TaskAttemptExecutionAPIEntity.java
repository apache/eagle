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
    // new added
    @Column("g")
    private long shuffleFinishTime;
    @Column("h")
    private long sortFinishTime;
    @Column("i")
    private long mapFinishTime;

    public long getShuffleFinishTime() {
        return shuffleFinishTime;
    }

    public void setShuffleFinishTime(long shuffleFinishTime) {
        this.shuffleFinishTime = shuffleFinishTime;
        valueChanged("shuffleFinishTime");
    }

    public long getSortFinishTime() {
        return sortFinishTime;
    }

    public void setSortFinishTime(long sortFinishTime) {
        this.sortFinishTime = sortFinishTime;
        valueChanged("sortFinishTime");
    }

    public long getMapFinishTime() {
        return mapFinishTime;
    }

    public void setMapFinishTime(long mapFinishTime) {
        this.mapFinishTime = mapFinishTime;
        valueChanged("mapFinishTime");
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
        valueChanged("taskStatus");
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

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
        valueChanged("duration");
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
        valueChanged("error");
    }

    public JobCounters getJobCounters() {
        return jobCounters;
    }

    public void setJobCounters(JobCounters jobCounters) {
        this.jobCounters = jobCounters;
        valueChanged("jobCounters");
    }
}
