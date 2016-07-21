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

import org.apache.eagle.jpm.mr.history.entities.JobConfig;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.entities.*;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.jpm.mr.history.parser.JHFMRVer1Parser.Keys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A new instance of JHFEventReaderBase will be created for each job history log file.
 */
public abstract class JHFEventReaderBase extends JobEntityCreationPublisher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JHFEventReaderBase.class);
    protected Map<String, String> m_baseTags;
    protected JobEventAPIEntity m_jobSubmitEventEntity;
    protected JobEventAPIEntity m_jobLaunchEventEntity;
    protected int m_numTotalMaps;
    protected int m_numTotalReduces;
    protected JobEventAPIEntity m_jobFinishEventEntity;
    protected JobExecutionAPIEntity m_jobExecutionEntity;
    protected Map<String, Long> m_taskStartTime;
    // taskAttemptID to task attempt startTime
    protected Map<String, Long> m_taskAttemptStartTime;

    // taskID to host mapping, for task it's the host where the last attempt runs on
    protected Map<String, String> m_taskRunningHosts;
    // hostname to rack mapping
    protected Map<String, String> m_host2RackMapping;

    protected String m_jobID;
    protected String m_jobName;
    protected String m_jobType;
    protected String m_normJobName;
    protected String m_user;
    protected String m_queueName;
    protected Long m_jobLauchTime;
    protected JobHistoryContentFilter m_filter;

    protected final List<HistoryJobEntityLifecycleListener> jobEntityLifecycleListeners = new ArrayList<>();

    protected final Configuration configuration;

    public Constants.JobType fetchJobType(Configuration config) {
        if (config.get(Constants.JobConfiguration.CASCADING_JOB) != null) { return Constants.JobType.CASCADING; }
        if (config.get(Constants.JobConfiguration.HIVE_JOB) != null) { return Constants.JobType.HIVE; }
        if (config.get(Constants.JobConfiguration.PIG_JOB) != null) { return Constants.JobType.PIG; }
        if (config.get(Constants.JobConfiguration.SCOOBI_JOB) != null) {return Constants.JobType.SCOOBI; }
        return Constants.JobType.NOTAVALIABLE;
    }

    /**
     * baseTags stores the basic tag name values which might be used for persisting various entities
     * baseTags includes: cluster, datacenter and jobName
     * baseTags are used for all job/task related entities
     * @param baseTags
     */
    public JHFEventReaderBase(Map<String, String> baseTags, Configuration configuration, JobHistoryContentFilter filter) {
        this.m_filter = filter;

        this.m_baseTags = baseTags;
        m_jobSubmitEventEntity = new JobEventAPIEntity();
        m_jobSubmitEventEntity.setTags(new HashMap<>(baseTags));

        m_jobLaunchEventEntity = new JobEventAPIEntity();
        m_jobLaunchEventEntity.setTags(new HashMap<>(baseTags));

        m_jobFinishEventEntity = new JobEventAPIEntity();
        m_jobFinishEventEntity.setTags(new HashMap<>(baseTags));

        m_jobExecutionEntity = new JobExecutionAPIEntity();
        m_jobExecutionEntity.setTags(new HashMap<>(baseTags));

        m_taskRunningHosts = new HashMap<>();

        m_host2RackMapping = new HashMap<>();

        m_taskStartTime = new HashMap<>();
        m_taskAttemptStartTime = new HashMap<>();

        this.configuration = configuration;

        if (this.configuration != null && this.m_jobType == null) {
            this.setJobType(fetchJobType(this.configuration).toString());
        }
    }

    public void register(HistoryJobEntityLifecycleListener lifecycleListener){
        this.jobEntityLifecycleListeners.add(lifecycleListener);
    }

    @Override
    public void close() throws IOException {
        // check if this job history file is complete
        if (m_jobExecutionEntity.getEndTime() == 0L) {
            throw new IOException(new JHFWriteNotCompletedException(m_jobID));
        }
        try {
            flush();
        } catch(Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void flush() throws Exception {
        super.flush();
        for (HistoryJobEntityLifecycleListener listener : this.jobEntityLifecycleListeners) {
            listener.flush();
        }
    }

    /**
       * @param id
       */
    private void setJobID(String id) {
        this.m_jobID = id;
    }

    private void setJobType(String jobType) {
        this.m_jobType = jobType;
    }

    protected void handleJob(EventType eventType, Map<Keys, String> values, Object totalCounters) throws Exception {
       String id = values.get(Keys.JOBID);

       if (m_jobID == null) {
           setJobID(id);
       } else if (!m_jobID.equals(id)) {
           String msg = "Current job ID '" + id + "' does not match previously stored value '" + m_jobID + "'";
           LOG.error(msg);
           throw new ImportException(msg);
       }

       if (values.get(Keys.SUBMIT_TIME) != null) {  // job submitted
           m_jobSubmitEventEntity.setTimestamp(Long.valueOf(values.get(Keys.SUBMIT_TIME)));
           m_user = values.get(Keys.USER);
           m_queueName = values.get(Keys.JOB_QUEUE);
           m_jobName = values.get(Keys.JOBNAME);
           m_normJobName = m_jobName;

           LOG.info("NormJobName of " + id + ": " + m_normJobName);

           m_jobSubmitEventEntity.getTags().put(MRJobTagName.USER.toString(), m_user);
           m_jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), m_jobID);
           m_jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), EagleJobStatus.SUBMITTED.name());
           m_jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), m_jobName);
           m_jobSubmitEventEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
           m_jobExecutionEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(),this.m_jobType);
           entityCreated(m_jobSubmitEventEntity);
       } else if(values.get(Keys.LAUNCH_TIME) != null) {  // job launched
           m_jobLaunchEventEntity.setTimestamp(Long.valueOf(values.get(Keys.LAUNCH_TIME)));
           m_jobLauchTime = m_jobLaunchEventEntity.getTimestamp();
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.USER.toString(), m_user);
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), m_jobID);
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), EagleJobStatus.LAUNCHED.name());
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), m_jobName);
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
           m_jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(),this.m_jobType);
           m_numTotalMaps = Integer.valueOf(values.get(Keys.TOTAL_MAPS));
           m_numTotalReduces = Integer.valueOf(values.get(Keys.TOTAL_REDUCES));
           entityCreated(m_jobLaunchEventEntity);
       } else if(values.get(Keys.FINISH_TIME) != null) {  // job finished
           m_jobFinishEventEntity.setTimestamp(Long.valueOf(values.get(Keys.FINISH_TIME)));
           m_jobFinishEventEntity.getTags().put(MRJobTagName.USER.toString(), m_user);
           m_jobFinishEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), m_jobID);
           m_jobFinishEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), values.get(Keys.JOB_STATUS));
           m_jobFinishEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), m_jobName);
           m_jobFinishEventEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
           m_jobFinishEventEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(),this.m_jobType);
           entityCreated(m_jobFinishEventEntity);

           // populate jobExecutionEntity entity
           m_jobExecutionEntity.getTags().put(MRJobTagName.USER.toString(), m_user);
           m_jobExecutionEntity.getTags().put(MRJobTagName.JOB_ID.toString(), m_jobID);
           m_jobExecutionEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), m_jobName);
           m_jobExecutionEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
           m_jobExecutionEntity.getTags().put(MRJobTagName.JOB_QUEUE.toString(), m_queueName);
           m_jobExecutionEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(),this.m_jobType);

           m_jobExecutionEntity.setCurrentState(values.get(Keys.JOB_STATUS));
           m_jobExecutionEntity.setStartTime(m_jobLaunchEventEntity.getTimestamp());
           m_jobExecutionEntity.setEndTime(m_jobFinishEventEntity.getTimestamp());
           m_jobExecutionEntity.setTimestamp(m_jobLaunchEventEntity.getTimestamp());
           if (values.get(Keys.FAILED_MAPS) != null) {
               // for Artemis
               m_jobExecutionEntity.setNumFailedMaps(Integer.valueOf(values.get(Keys.FAILED_MAPS)));
           }
           if (values.get(Keys.FAILED_REDUCES) != null) {
               // for Artemis
               m_jobExecutionEntity.setNumFailedReduces(Integer.valueOf(values.get(Keys.FAILED_REDUCES)));
           }
           m_jobExecutionEntity.setNumFinishedMaps(Integer.valueOf(values.get(Keys.FINISHED_MAPS)));
           m_jobExecutionEntity.setNumFinishedReduces(Integer.valueOf(values.get(Keys.FINISHED_REDUCES)));
           m_jobExecutionEntity.setNumTotalMaps(m_numTotalMaps);
           m_jobExecutionEntity.setNumTotalReduces(m_numTotalReduces);
           if (values.get(Keys.COUNTERS) != null || totalCounters != null) {
               m_jobExecutionEntity.setJobCounters(parseCounters(totalCounters));
           }
           entityCreated(m_jobExecutionEntity);
       }
    }

    private void entityCreated(JobBaseAPIEntity entity) throws Exception {
        for (HistoryJobEntityLifecycleListener lifecycleListener: this.jobEntityLifecycleListeners) {
            lifecycleListener.jobEntityCreated(entity);
        }

        // job finished when passing JobExecutionAPIEntity: m_jobExecutionEntity
        if (entity == this.m_jobExecutionEntity) {
            for (HistoryJobEntityLifecycleListener lifecycleListener : this.jobEntityLifecycleListeners) {
                lifecycleListener.jobFinish();
            }
        }

        super.notifiyListeners(entity);
    }

    protected abstract JobCounters parseCounters(Object value) throws IOException;

    /**
      * for one task ID, it has several sequential task events, i.e.
      * task_start -> task_attempt_start -> task_attempt_finish -> task_attempt_start -> task_attempt_finish -> ... -> task_end
      * @param values
      * @throws IOException
      */
    @SuppressWarnings("serial")
    protected void handleTask(RecordTypes recType, EventType eventType, final Map<Keys, String> values, Object counters) throws Exception {
        String taskAttemptID = values.get(Keys.TASK_ATTEMPT_ID);
        String startTime = values.get(Keys.START_TIME);
        String finishTime = values.get(Keys.FINISH_TIME);
        final String taskType = values.get(Keys.TASK_TYPE);
        final String taskID = values.get(Keys.TASKID);

        Map<String, String> taskBaseTags = new HashMap<String, String>(){{
            put(MRJobTagName.TASK_TYPE.toString(), taskType);
            put(MRJobTagName.USER.toString(), m_user);
            //put(MRJobTagName.JOB_NAME.toString(), _jobName);
            put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
            put(MRJobTagName.JOB_TYPE.toString(), m_jobType);
            put(MRJobTagName.JOB_ID.toString(), m_jobID);
            put(MRJobTagName.TASK_ID.toString(), taskID);
        }};
        taskBaseTags.putAll(m_baseTags);
        if (recType == RecordTypes.Task && startTime != null) { // task start, no host is assigned yet
            m_taskStartTime.put(taskID, Long.valueOf(startTime));
        } else if (recType == RecordTypes.Task && finishTime != null) { // task finish
            // task execution entity setup
            TaskExecutionAPIEntity entity = new TaskExecutionAPIEntity();
            Map<String, String> taskExecutionTags = new HashMap<>(taskBaseTags);
            String hostname = m_taskRunningHosts.get(taskID);
            hostname = (hostname == null) ? "" : hostname; // TODO if task fails, then no hostname
            taskExecutionTags.put(MRJobTagName.HOSTNAME.toString(), hostname);
            taskExecutionTags.put(MRJobTagName.RACK.toString(), m_host2RackMapping.get(hostname));
            entity.setTags(taskExecutionTags);
            entity.setStartTime(m_taskStartTime.get(taskID));
            entity.setEndTime(Long.valueOf(finishTime));
            entity.setDuration(entity.getEndTime() - entity.getStartTime());
            entity.setTimestamp(m_jobLauchTime);
            entity.setError(values.get(Keys.ERROR));
            entity.setTaskStatus(values.get(Keys.TASK_STATUS));
            if (values.get(Keys.COUNTERS) != null || counters != null) {
                entity.setJobCounters(parseCounters(counters));
            }
            entityCreated(entity);
            //_taskStartTime.remove(taskID); // clean this taskID
        } else if ((recType == RecordTypes.MapAttempt || recType == RecordTypes.ReduceAttempt) && startTime != null) { // task attempt start
            m_taskAttemptStartTime.put(taskAttemptID, Long.valueOf(startTime));
        } else if ((recType == RecordTypes.MapAttempt || recType == RecordTypes.ReduceAttempt) && finishTime != null) {   // task attempt finish
            TaskAttemptExecutionAPIEntity entity = new TaskAttemptExecutionAPIEntity();
            Map<String, String> taskAttemptExecutionTags = new HashMap<>(taskBaseTags);
            entity.setTags(taskAttemptExecutionTags);
            String hostname = values.get(Keys.HOSTNAME);
            String rack = values.get(Keys.RACK);
            taskAttemptExecutionTags.put(MRJobTagName.HOSTNAME.toString(), hostname);
            taskAttemptExecutionTags.put(MRJobTagName.RACK.toString(), rack);
            // put last attempt's hostname to task level
            m_taskRunningHosts.put(taskID, hostname);
            // it is very likely that an attempt ID could be both succeeded and failed due to M/R system
            // in this case, we should ignore this attempt?
            if (m_taskAttemptStartTime.get(taskAttemptID) == null) {
                LOG.warn("task attemp has consistency issue " + taskAttemptID);
                return;
            }
            entity.setStartTime(m_taskAttemptStartTime.get(taskAttemptID));
            entity.setEndTime(Long.valueOf(finishTime));
            entity.setTimestamp(m_jobLauchTime);
            entity.setDuration(entity.getEndTime() - entity.getStartTime());
            entity.setTaskStatus(values.get(Keys.TASK_STATUS));
            entity.setError(values.get(Keys.ERROR));
            if (values.get(Keys.COUNTERS) != null || counters != null) {  // when task is killed, COUNTERS does not exist
                //entity.setJobCounters(parseCounters(values.get(Keys.COUNTERS)));
                entity.setJobCounters(parseCounters(counters));
            }
            entity.setTaskAttemptID(taskAttemptID);
            entityCreated(entity);
            m_taskAttemptStartTime.remove(taskAttemptID);
        } else {
            // silently ignore
            LOG.warn("It's an exceptional case ?");
        }
    }

    public void parseConfiguration() throws Exception {
        Map<String, String> prop = new TreeMap<>();

        if (m_filter.acceptJobConfFile()) {
            Iterator<Map.Entry<String, String> > iter = configuration.iterator();
            while (iter.hasNext()) {
                String key = iter.next().getKey();
                if (included(key) && !excluded(key))
                    prop.put(key, configuration.get(key));
            }
        }

        // check must-have keys are within prop
        if (matchMustHaveKeyPatterns(prop)) {
            JobConfigurationAPIEntity jobConfigurationEntity = new JobConfigurationAPIEntity();
            jobConfigurationEntity.setTags(new HashMap<>(m_baseTags));
            jobConfigurationEntity.getTags().put(MRJobTagName.USER.toString(), m_user);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_ID.toString(), m_jobID);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), m_jobName);
            jobConfigurationEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), m_normJobName);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), m_jobType);
            jobConfigurationEntity.setTimestamp(m_jobLaunchEventEntity.getTimestamp());

            JobConfig jobConfig = new JobConfig();
            jobConfig.setConfig(prop);
            jobConfigurationEntity.setJobConfig(jobConfig);
            jobConfigurationEntity.setConfigJobName(m_normJobName);
            entityCreated(jobConfigurationEntity);
        }
    }

    private boolean matchMustHaveKeyPatterns(Map<String, String> prop) {
        if (m_filter.getMustHaveJobConfKeyPatterns() == null) {
            return true;
        }

        for (Pattern p : m_filter.getMustHaveJobConfKeyPatterns()) {
            boolean matched = false;
            for (String key : prop.keySet()) {
                if (p.matcher(key).matches()) {
                    matched = true;
                    break;
                }
            }
            if (!matched)
                return false;
        }
        return true;
    }

    private boolean included(String key) {
        if (m_filter.getJobConfKeyInclusionPatterns() == null)
            return true;
        for (Pattern p : m_filter.getJobConfKeyInclusionPatterns()) {
            Matcher m = p.matcher(key);
            if (m.matches()) {
                LOG.info("include key: " + p.toString());
                return true;
            }
        }
        return false;
    }

    private boolean excluded(String key) {
        if (m_filter.getJobConfKeyExclusionPatterns() == null)
            return false;
        for (Pattern p : m_filter.getJobConfKeyExclusionPatterns()) {
            Matcher m = p.matcher(key);
            if (m.matches())
                return true;
        }
        return false;
    }
}
