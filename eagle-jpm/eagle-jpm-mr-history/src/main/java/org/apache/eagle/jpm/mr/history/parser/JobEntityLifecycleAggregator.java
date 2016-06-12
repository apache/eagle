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

package org.apache.eagle.jpm.mr.history.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.jpm.mr.history.common.JPAConstants;
import org.apache.eagle.jpm.mr.history.entities.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.history.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.history.entities.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.history.jobcounter.JobCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JobEntityLifecycleAggregator implements HistoryJobEntityLifecycleListener {
    private final static Logger LOG = LoggerFactory.getLogger(JobEntityLifecycleAggregator.class);
    private JobExecutionAPIEntity m_jobExecutionAPIEntity;
    private final JobCounterAggregateFunction m_mapTaskAttemptCounterAgg;
    private final JobCounterAggregateFunction m_reduceTaskAttemptCounterAgg;

    private final JobCounterAggregateFunction m_mapFileSystemCounterAgg;
    private final JobCounterAggregateFunction m_reduceFileSystemTaskCounterAgg;

    private long m_mapAttemptDuration = 0;
    private long m_reduceAttemptDuration = 0;
    private boolean jobFinished = false;

    public JobEntityLifecycleAggregator() {
        this.m_mapTaskAttemptCounterAgg = new JobCounterSumFunction();
        this.m_reduceTaskAttemptCounterAgg = new JobCounterSumFunction();
        this.m_mapFileSystemCounterAgg = new JobCounterSumFunction();
        this.m_reduceFileSystemTaskCounterAgg = new JobCounterSumFunction();
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        if (entity != null ) {
            if (entity instanceof TaskAttemptExecutionAPIEntity) {
                taskAttemptEntityCreated((TaskAttemptExecutionAPIEntity) entity);
            } else if(entity instanceof JobExecutionAPIEntity) {
                this.m_jobExecutionAPIEntity = (JobExecutionAPIEntity) entity;
            }
        }
    }

    @Override
    public void jobFinish() {
        try {
            if (m_jobExecutionAPIEntity == null) {
                throw new IOException("No JobExecutionAPIEntity found before flushing");
            }

            LOG.debug("Updating aggregated task attempts to job level counters");

            JobCounters jobCounters = m_jobExecutionAPIEntity.getJobCounters();

            if (jobCounters == null) {
                LOG.warn("no job counter found for "+this.m_jobExecutionAPIEntity);
                jobCounters = new JobCounters();
            }

            Map<String,Map<String,Long>> counters = jobCounters.getCounters();

            Map<String,Long> mapTaskAttemptCounter = this.m_mapTaskAttemptCounterAgg.result();
            if (mapTaskAttemptCounter == null) mapTaskAttemptCounter = new HashMap<>();
            mapTaskAttemptCounter.put(JPAConstants.TaskAttemptCounter.TASK_ATTEMPT_DURATION.toString(), this.m_mapAttemptDuration);
            counters.put(JPAConstants.MAP_TASK_ATTEMPT_COUNTER,mapTaskAttemptCounter);

            Map<String,Long> reduceTaskAttemptCounter = this.m_reduceTaskAttemptCounterAgg.result();
            if (reduceTaskAttemptCounter == null) reduceTaskAttemptCounter = new HashMap<>();
            reduceTaskAttemptCounter.put(JPAConstants.TaskAttemptCounter.TASK_ATTEMPT_DURATION.toString(), this.m_reduceAttemptDuration);
            counters.put(JPAConstants.REDUCE_TASK_ATTEMPT_COUNTER,reduceTaskAttemptCounter);

            counters.put(JPAConstants.MAP_TASK_ATTEMPT_FILE_SYSTEM_COUNTER, this.m_mapFileSystemCounterAgg.result());
            counters.put(JPAConstants.REDUCE_TASK_ATTEMPT_FILE_SYSTEM_COUNTER,this.m_reduceFileSystemTaskCounterAgg.result());

            jobCounters.setCounters(counters);

            m_jobExecutionAPIEntity.setJobCounters(jobCounters);
            jobFinished = true;
        } catch (Exception e) {
            LOG.error("Failed to update job execution entity: " + this.m_jobExecutionAPIEntity.toString() + ", due to " + e.getMessage(), e);
        }
    }

    private void taskAttemptEntityCreated(TaskAttemptExecutionAPIEntity entity) {
        JobCounters jobCounters = entity.getJobCounters();
        String taskType = entity.getTags().get(JPAConstants.JOB_TASK_TYPE_TAG);

        if (taskType != null && jobCounters != null && jobCounters.getCounters() != null) {
            if (JPAConstants.TaskType.MAP.toString().equals(taskType.toUpperCase())) {
                m_mapAttemptDuration += entity.getDuration();
                this.m_mapTaskAttemptCounterAgg.accumulate(jobCounters.getCounters().get(JPAConstants.TASK_COUNTER));
                this.m_mapFileSystemCounterAgg.accumulate(jobCounters.getCounters().get(JPAConstants.FILE_SYSTEM_COUNTER));
                return;
            } else if (JPAConstants.TaskType.REDUCE.toString().equals(taskType.toUpperCase())) {
                m_reduceAttemptDuration += entity.getDuration();
                this.m_reduceTaskAttemptCounterAgg.accumulate(jobCounters.getCounters().get(JPAConstants.TASK_COUNTER));
                this.m_reduceFileSystemTaskCounterAgg.accumulate(jobCounters.getCounters().get(JPAConstants.FILE_SYSTEM_COUNTER));
                return;
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LOG.warn("Unknown task type of task attempt execution entity: "+objectMapper.writeValueAsString(entity));
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(),e);
        }
    }

    @Override
    public void flush() throws Exception {
        if (!this.jobFinished) {
            this.jobFinish();
        }
    }

    interface JobCounterAggregateFunction {
        void accumulate(Map<String, Long> mapCounters);
        Map<String,Long> result();
    }

    static class JobCounterSumFunction implements JobCounterAggregateFunction {
        final Map<String, Long> result;

        public JobCounterSumFunction(){
            result = new HashMap<>();
        }

        /**
         * @param counters
         */
        @Override
        public void accumulate(Map<String, Long> counters) {
            if (counters != null) {
                for (Map.Entry<String,Long> taskEntry: counters.entrySet()) {
                    String counterName = taskEntry.getKey();
                    long counterValue = taskEntry.getValue();

                    if (!result.containsKey(counterName)) {
                        result.put(counterName, counterValue);
                    } else {
                        long preValue = result.get(counterName);
                        result.put(counterName, preValue + counterValue);
                    }
                }
            }
        }

        @Override
        public Map<String, Long> result() {
            return result;
        }
    }
}
