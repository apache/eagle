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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Table("eglesprk_jobs")
@ColumnFamily("f")
@Prefix("sprkjob")
@Service(Constants.SPARK_JOB_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site","sparkAppId", "sparkAppAttemptId", "sparkAppName", "normsparkAppName", "jobId","user", "queue"})
@Partition({"site"})
public class SparkJob extends TaggedLogAPIEntity {

    @Column("a")
    private long  submissionTime;
    @Column("b")
    private long completionTime;
    @Column("c")
    private int numStages = 0;
    @Column("d")
    private String status;
    @Column("e")
    private int numTask = 0;
    @Column("f")
    private int numActiveTasks = 0;
    @Column("g")
    private int numCompletedTasks = 0;
    @Column("h")
    private int numSkippedTasks = 0;
    @Column("i")
    private int numFailedTasks = 0;
    @Column("j")
    private int numActiveStages = 0;
    @Column("k")
    private int numCompletedStages = 0;
    @Column("l")
    private int numSkippedStages = 0;
    @Column("m")
    private int numFailedStages = 0;

    public long getSubmissionTime() {
        return submissionTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public int getNumStages() {
        return numStages;
    }

    public String getStatus() {
        return status;
    }

    public int getNumTask() {
        return numTask;
    }

    public int getNumActiveTasks() {
        return numActiveTasks;
    }

    public int getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public int getNumSkippedTasks() {
        return numSkippedTasks;
    }

    public int getNumFailedTasks() {
        return numFailedTasks;
    }

    public int getNumActiveStages() {
        return numActiveStages;
    }

    public int getNumCompletedStages() {
        return numCompletedStages;
    }

    public int getNumSkippedStages() {
        return numSkippedStages;
    }

    public int getNumFailedStages() {
        return numFailedStages;
    }

    public void setSubmissionTime(long submissionTime) {
        this.submissionTime = submissionTime;
        this.valueChanged("submissionTime");
    }

    public void setCompletionTime(long completionTime) {
        this.completionTime = completionTime;
        this.valueChanged("completionTime");
    }

    public void setNumStages(int numStages) {
        this.numStages = numStages;
        this.valueChanged("numStages");
    }

    public void setStatus(String status) {
        this.status = status;
        this.valueChanged("status");
    }

    public void setNumTask(int numTask) {
        this.numTask = numTask;
        this.valueChanged("numTask");
    }

    public void setNumActiveTasks(int numActiveTasks) {
        this.numActiveTasks = numActiveTasks;
        this.valueChanged("numActiveTasks");
    }

    public void setNumCompletedTasks(int numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
        this.valueChanged("numCompletedTasks");
    }

    public void setNumSkippedTasks(int numSkippedTasks) {
        this.numSkippedTasks = numSkippedTasks;
        this.valueChanged("numSkippedTasks");
    }

    public void setNumFailedTasks(int numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
        this.valueChanged("numFailedTasks");
    }

    public void setNumActiveStages(int numActiveStages) {
        this.numActiveStages = numActiveStages;
        this.valueChanged("numActiveStages");
    }

    public void setNumCompletedStages(int numCompletedStages) {
        this.numCompletedStages = numCompletedStages;
        this.valueChanged("numCompletedStages");
    }

    public void setNumSkippedStages(int numSkippedStages) {
        this.numSkippedStages = numSkippedStages;
        this.valueChanged("numSkippedStages");
    }

    public void setNumFailedStages(int numFailedStages) {
        this.numFailedStages = numFailedStages;
        this.valueChanged("numFailedStages");
    }
}
