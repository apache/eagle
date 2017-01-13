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

package org.apache.eagle.jpm.analyzer;

import java.util.HashMap;
import java.util.Map;

/**
 * will refactor later if other types of job needs this.
 * AnalyzerEntity for each job needed to be analysised
 */
public class AnalyzerEntity {
    private String jobDefId;
    private String jobId;
    private String siteId;
    private String userId;

    private long startTime;
    private long endTime;
    private long durationTime;
    private String currentState;
    private double progress;

    private Map<String, Object> jobConfig = new HashMap<>();

    private Map<String, Object> jobMeta = new HashMap<>();

    public String getJobDefId() {
        return jobDefId;
    }

    public void setJobDefId(String jobDefId) {
        this.jobDefId = jobDefId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getDurationTime() {
        return durationTime;
    }

    public void setDurationTime(long durationTime) {
        this.durationTime = durationTime;
    }

    public String getCurrentState() {
        return currentState;
    }

    public void setCurrentState(String currentState) {
        this.currentState = currentState;
    }

    public Map<String, Object> getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(Map<String, Object> jobConfig) {
        this.jobConfig = jobConfig;
    }

    public Map<String, Object> getJobMeta() {
        return jobMeta;
    }

    public void setJobMeta(Map<String, Object> jobMeta) {
        this.jobMeta = jobMeta;
    }

    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
    }
}