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

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.meta.model.MapReduceAnalyzerEntity;
import org.apache.eagle.jpm.analyzer.mr.MRJobPerformanceAnalyzer;
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

import static org.apache.eagle.jpm.util.MRJobTagName.TASK_ATTEMPT_ID;
import static org.apache.eagle.jpm.util.MRJobTagName.TASK_ID;

/*
 * JobEventCounterListener provides an interface to add job/task counter analyzers
 */
public class JobSuggestionListener implements HistoryJobEntityCreationListener {
    private static final Logger LOG = LoggerFactory.getLogger(JobSuggestionListener.class);

    private MapReduceAnalyzerEntity info;
    private MRJobPerformanceAnalyzer<MapReduceAnalyzerEntity> analyzer;

    public JobSuggestionListener(Config config) {
        this.info = new MapReduceAnalyzerEntity();
        this.analyzer = new MRJobPerformanceAnalyzer<>(config);
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        if (entity instanceof TaskExecutionAPIEntity) {
            info.getTasksMap().put(entity.getTags().get(TASK_ID.toString()), (TaskExecutionAPIEntity) entity);
        } else if (entity instanceof TaskAttemptExecutionAPIEntity) {
            info.getCompletedTaskAttemptsMap().put(entity.getTags().get(TASK_ATTEMPT_ID.toString()), (TaskAttemptExecutionAPIEntity) entity);
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
            info.setJobName(jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_NAME.toString())) ;
            info.setJobQueueName(jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_QUEUE.toString()));
            info.setJobType(jobExecutionAPIEntity.getTags().get(MRJobTagName.JOB_TYPE.toString()));
            info.setFinishedMaps(jobExecutionAPIEntity.getNumFinishedMaps());
            info.setFinishedReduces(jobExecutionAPIEntity.getNumFinishedReduces());
            info.setFailedReduces(jobExecutionAPIEntity.getNumFailedReduces());
            info.setFailedMaps(jobExecutionAPIEntity.getNumFailedMaps());
            info.setTotalMaps(jobExecutionAPIEntity.getNumTotalMaps());
            info.setTotalReduces(jobExecutionAPIEntity.getNumTotalReduces());
            info.setProgress(100);
        }
    }

    public void jobConfigCreated(Configuration configuration) {
        info.setJobConf(configuration);
    }

    public void jobCountersCreated(JobCounters totalCounters, JobCounters mapCounters, JobCounters reduceCounters) {
        info.setTotalCounters(totalCounters);
        info.setReduceCounters(reduceCounters);
        info.setMapCounters(mapCounters);
    }

    @Override
    public void flush() throws Exception {
        if (info.getTotalCounters() == null) {
            LOG.warn("Total counters of Job {} is null", info.getJobId());
            return;
        }
        if (info.getMapCounters() == null && info.getReduceCounters() == null) {
            LOG.warn("Map/Reduce task counters of Job {} are null", info.getJobId());
            return;
        }
        analyzer.analyze(info);
    }
}
