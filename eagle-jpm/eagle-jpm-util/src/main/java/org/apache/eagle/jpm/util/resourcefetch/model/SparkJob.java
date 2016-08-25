/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.util.resourcefetch.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkJob {
    private int jobId;
    private String name;
    private String submissionTime;
    private String completionTime;
    private List<Integer> stageIds;
    private String status;
    private int numTasks;
    private int numActiveTasks;
    private int numCompletedTasks;
    private int numSkippedTasks;
    private int numFailedTasks;
    private int numActiveStages;
    private int numCompletedStages;
    private int numSkippedStages;
    private int numFailedStages;

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(String submissionTime) {
        this.submissionTime = submissionTime;
    }

    public String getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(String completionTime) {
        this.completionTime = completionTime;
    }

    public List<Integer> getStageIds() {
        return stageIds;
    }

    public void setStageIds(List<Integer> stageIds) {
        this.stageIds = stageIds;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public void setNumTasks(int numTasks) {
        this.numTasks = numTasks;
    }

    public int getNumActiveTasks() {
        return numActiveTasks;
    }

    public void setNumActiveTasks(int numActiveTasks) {
        this.numActiveTasks = numActiveTasks;
    }

    public int getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public void setNumCompletedTasks(int numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
    }

    public int getNumSkippedTasks() {
        return numSkippedTasks;
    }

    public void setNumSkippedTasks(int numSkippedTasks) {
        this.numSkippedTasks = numSkippedTasks;
    }

    public int getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(int numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }

    public int getNumActiveStages() {
        return numActiveStages;
    }

    public void setNumActiveStages(int numActiveStages) {
        this.numActiveStages = numActiveStages;
    }

    public int getNumCompletedStages() {
        return numCompletedStages;
    }

    public void setNumCompletedStages(int numCompletedStages) {
        this.numCompletedStages = numCompletedStages;
    }

    public int getNumSkippedStages() {
        return numSkippedStages;
    }

    public void setNumSkippedStages(int numSkippedStages) {
        this.numSkippedStages = numSkippedStages;
    }

    public int getNumFailedStages() {
        return numFailedStages;
    }

    public void setNumFailedStages(int numFailedStages) {
        this.numFailedStages = numFailedStages;
    }
}
