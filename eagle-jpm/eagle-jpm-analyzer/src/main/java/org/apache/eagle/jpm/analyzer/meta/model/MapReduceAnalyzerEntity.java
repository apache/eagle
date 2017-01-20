/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.meta.model;

import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class MapReduceAnalyzerEntity extends AnalyzerEntity {
    private String jobName;
    private String jobQueueName;
    private String jobType;
    private int totalMaps;
    private int totalReduces;
    private int failedMaps;
    private int failedReduces;
    private int finishedMaps;
    private int finishedReduces;
    private JobCounters totalCounters;
    private JobCounters mapCounters;
    private JobCounters reduceCounters;
    private Map<String, TaskExecutionAPIEntity> tasksMap;
    private Map<String, TaskAttemptExecutionAPIEntity> completedTaskAttemptsMap;
    private Configuration jobConf;

    public MapReduceAnalyzerEntity() {
        this.setEndTime(-1);
        this.setStartTime(-1);
        finishedMaps = finishedReduces = 0;
        jobName = jobQueueName = "";
        tasksMap = new HashMap<>();
        completedTaskAttemptsMap = new HashMap<>();
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobQueueName() {
        return jobQueueName;
    }

    public String getJobType() {
        return jobType;
    }

    public int getTotalMaps() {
        return totalMaps;
    }

    public int getTotalReduces() {
        return totalReduces;
    }

    public int getFailedMaps() {
        return failedMaps;
    }

    public int getFailedReduces() {
        return failedReduces;
    }

    public int getFinishedMaps() {
        return finishedMaps;
    }

    public int getFinishedReduces() {
        return finishedReduces;
    }

    public JobCounters getTotalCounters() {
        return totalCounters;
    }

    public JobCounters getMapCounters() {
        return mapCounters;
    }

    public JobCounters getReduceCounters() {
        return reduceCounters;
    }

    public Map<String, TaskExecutionAPIEntity> getTasksMap() {
        return tasksMap;
    }

    public Map<String, TaskAttemptExecutionAPIEntity> getCompletedTaskAttemptsMap() {
        return completedTaskAttemptsMap;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobQueueName(String jobQueueName) {
        this.jobQueueName = jobQueueName;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public void setTotalMaps(int totalMaps) {
        this.totalMaps = totalMaps;
    }

    public void setTotalReduces(int totalReduces) {
        this.totalReduces = totalReduces;
    }

    public void setFailedMaps(int failedMaps) {
        this.failedMaps = failedMaps;
    }

    public void setFailedReduces(int failedReduces) {
        this.failedReduces = failedReduces;
    }

    public void setFinishedMaps(int finishedMaps) {
        this.finishedMaps = finishedMaps;
    }

    public void setFinishedReduces(int finishedReduces) {
        this.finishedReduces = finishedReduces;
    }

    public void setTotalCounters(JobCounters totalCounters) {
        this.totalCounters = totalCounters;
    }

    public void setMapCounters(JobCounters mapCounters) {
        this.mapCounters = mapCounters;
    }

    public void setReduceCounters(JobCounters reduceCounters) {
        this.reduceCounters = reduceCounters;
    }

    public void setTasksMap(Map<String, TaskExecutionAPIEntity> tasksMap) {
        this.tasksMap = tasksMap;
    }

    public void setCompletedTaskAttemptsMap(Map<String, TaskAttemptExecutionAPIEntity> completedTaskAttemptsMap) {
        this.completedTaskAttemptsMap = completedTaskAttemptsMap;
    }

    public Configuration getJobConf() {
        return jobConf;
    }

    public void setJobConf(Configuration jobConf) {
        this.jobConf = jobConf;
    }

}
