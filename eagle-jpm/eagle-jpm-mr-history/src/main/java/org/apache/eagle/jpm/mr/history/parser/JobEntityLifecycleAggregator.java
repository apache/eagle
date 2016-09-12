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

import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JobEntityLifecycleAggregator implements HistoryJobEntityLifecycleListener {
    private static final Logger LOG = LoggerFactory.getLogger(JobEntityLifecycleAggregator.class);
    private JobExecutionAPIEntity jobExecutionAPIEntity;
    private final JobCounterAggregateFunction mapTaskAttemptCounterAgg;
    private final JobCounterAggregateFunction reduceTaskAttemptCounterAgg;

    private final JobCounterAggregateFunction mapFileSystemCounterAgg;
    private final JobCounterAggregateFunction reduceFileSystemTaskCounterAgg;

    private long mapAttemptDuration = 0;
    private long reduceAttemptDuration = 0;
    private boolean jobFinished = false;

    public JobEntityLifecycleAggregator() {
        this.mapTaskAttemptCounterAgg = new JobCounterSumFunction();
        this.reduceTaskAttemptCounterAgg = new JobCounterSumFunction();
        this.mapFileSystemCounterAgg = new JobCounterSumFunction();
        this.reduceFileSystemTaskCounterAgg = new JobCounterSumFunction();
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        if (entity != null) {
            if (entity instanceof TaskAttemptExecutionAPIEntity) {
                taskAttemptEntityCreated((TaskAttemptExecutionAPIEntity) entity);
            } else if (entity instanceof JobExecutionAPIEntity) {
                this.jobExecutionAPIEntity = (JobExecutionAPIEntity) entity;
            }
        }
    }

    @Override
    public void jobFinish() {
        try {
            if (jobExecutionAPIEntity == null) {
                throw new IOException("No JobExecutionAPIEntity found before flushing");
            }

            LOG.debug("Updating aggregated task attempts to job level counters");

            JobCounters jobCounters = jobExecutionAPIEntity.getJobCounters();

            if (jobCounters == null) {
                LOG.warn("no job counter found for " + this.jobExecutionAPIEntity);
                jobCounters = new JobCounters();
            }

            Map<String, Map<String, Long>> counters = jobCounters.getCounters();

            Map<String, Long> mapTaskAttemptCounter = this.mapTaskAttemptCounterAgg.result();
            if (mapTaskAttemptCounter == null) {
                mapTaskAttemptCounter = new HashMap<>();
            }
            mapTaskAttemptCounter.put(Constants.TaskAttemptCounter.TASK_ATTEMPT_DURATION.toString(), this.mapAttemptDuration);
            counters.put(Constants.MAP_TASK_ATTEMPT_COUNTER, mapTaskAttemptCounter);

            Map<String, Long> reduceTaskAttemptCounter = this.reduceTaskAttemptCounterAgg.result();
            if (reduceTaskAttemptCounter == null) {
                reduceTaskAttemptCounter = new HashMap<>();
            }
            reduceTaskAttemptCounter.put(Constants.TaskAttemptCounter.TASK_ATTEMPT_DURATION.toString(), this.reduceAttemptDuration);
            counters.put(Constants.REDUCE_TASK_ATTEMPT_COUNTER, reduceTaskAttemptCounter);

            counters.put(Constants.MAP_TASK_ATTEMPT_FILE_SYSTEM_COUNTER, this.mapFileSystemCounterAgg.result());
            counters.put(Constants.REDUCE_TASK_ATTEMPT_FILE_SYSTEM_COUNTER, this.reduceFileSystemTaskCounterAgg.result());

            jobCounters.setCounters(counters);

            jobExecutionAPIEntity.setJobCounters(jobCounters);
            jobFinished = true;
        } catch (Exception e) {
            LOG.error("Failed to update job execution entity: " + this.jobExecutionAPIEntity.toString() + ", due to " + e.getMessage(), e);
        }
    }

    private void taskAttemptEntityCreated(TaskAttemptExecutionAPIEntity entity) {
        JobCounters jobCounters = entity.getJobCounters();
        String taskType = entity.getTags().get(Constants.JOB_TASK_TYPE_TAG);

        if (taskType != null && jobCounters != null && jobCounters.getCounters() != null) {
            if (Constants.TaskType.MAP.toString().equals(taskType.toUpperCase())) {
                mapAttemptDuration += entity.getDuration();
                this.mapTaskAttemptCounterAgg.accumulate(jobCounters.getCounters().get(Constants.TASK_COUNTER));
                this.mapFileSystemCounterAgg.accumulate(jobCounters.getCounters().get(Constants.FILE_SYSTEM_COUNTER));
                return;
            } else if (Constants.TaskType.REDUCE.toString().equals(taskType.toUpperCase())) {
                reduceAttemptDuration += entity.getDuration();
                this.reduceTaskAttemptCounterAgg.accumulate(jobCounters.getCounters().get(Constants.TASK_COUNTER));
                this.reduceFileSystemTaskCounterAgg.accumulate(jobCounters.getCounters().get(Constants.FILE_SYSTEM_COUNTER));
                return;
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LOG.warn("Unknown task type of task attempt execution entity: " + objectMapper.writeValueAsString(entity));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
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

        Map<String, Long> result();
    }

    static class JobCounterSumFunction implements JobCounterAggregateFunction {
        final Map<String, Long> result;

        public JobCounterSumFunction() {
            result = new HashMap<>();
        }
        
        @Override
        public void accumulate(Map<String, Long> counters) {
            if (counters != null) {
                for (Map.Entry<String, Long> taskEntry : counters.entrySet()) {
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
