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

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.eagle.jpm.analyzer.AnalyzerEntity;
import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.jpm.mr.history.parser.JHFEventReaderBase.Keys;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * JobEventCounterListener provides an interface to add job/task counter analyzers
 */
public class JobSuggestionListener implements HistoryJobEntityCreationListener {
    private static final Logger LOG = LoggerFactory.getLogger(JobSuggestionListener.class);

    private MapReduceAnalyzerEntity info = new MapReduceAnalyzerEntity();
    private Configuration jobConf;

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        if (entity instanceof TaskExecutionAPIEntity) {
            info.getTasksMap().put(entity.getTags().get(Keys.TASKID), (TaskExecutionAPIEntity) entity);
        } else if (entity instanceof TaskAttemptExecutionAPIEntity) {
            info.getCompletedTaskAttemptsMap().put(entity.getTags().get(Keys.TASK_ATTEMPT_ID), (TaskAttemptExecutionAPIEntity) entity);
        } else if (entity instanceof JobExecutionAPIEntity) {
            JobExecutionAPIEntity jobExecutionAPIEntity = (JobExecutionAPIEntity) entity;
            info.setCurrentState(jobExecutionAPIEntity.getCurrentState());
            info.setStartTime(jobExecutionAPIEntity.getStartTime());
            info.setEndTime(jobExecutionAPIEntity.getEndTime());
            info.setDurationTime(jobExecutionAPIEntity.getDurationTime());
            info.setUserId(jobExecutionAPIEntity.getTags().get(MRJobTagName.USER.toString()));
            info.setJobId(jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_ID.toString()));
            info.setJobDefId(jobExecutionAPIEntity.getTags().get(MRJobTagName.JOD_DEF_ID.toString()));
            info.setSiteId(jobExecutionAPIEntity.getTags().get(MRJobTagName.SITE.toString()));
            info.jobName = jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_NAME.toString());
            info.jobQueueName = jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_QUEUE.toString());
            info.jobType = jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_TYPE.toString());
            info.finishedMaps = jobExecutionAPIEntity.getNumFinishedMaps();
            info.finishedReduces = jobExecutionAPIEntity.getNumFinishedReduces();
            info.failedReduces = jobExecutionAPIEntity.getNumFailedReduces();
            info.failedMaps = jobExecutionAPIEntity.getNumFailedMaps();
            info.totalMaps = jobExecutionAPIEntity.getNumTotalMaps();
            info.totalReduces = jobExecutionAPIEntity.getNumTotalReduces();
        }
    }

    public void jobConfigCreated(Configuration configuration) {
        this.jobConf = configuration;
    }

    public void jobCountersCreated(JobCounters totalCounters, JobCounters mapCounters, JobCounters reduceCounters) {
        info.totalCounters = totalCounters;
        info.reduceCounters = reduceCounters;
        info.mapCounters = mapCounters;
    }

    @Override
    public void flush() throws Exception {

    }

    public static class MapReduceAnalyzerEntity extends AnalyzerEntity {
        String jobName;
        String jobQueueName;
        String jobType;
        int totalMaps;
        int totalReduces;
        int failedMaps;
        int failedReduces;
        int finishedMaps;
        int finishedReduces;
        JobCounters totalCounters;
        JobCounters mapCounters;
        JobCounters reduceCounters;
        Map<String, TaskExecutionAPIEntity> tasksMap;
        Map<String, TaskAttemptExecutionAPIEntity> completedTaskAttemptsMap;

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
    }

}
