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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.metrics.JobCounterMetricsGenerator;
import org.apache.eagle.jpm.mr.history.parser.JHFMRVer1Parser.Keys;
import org.apache.eagle.jpm.mr.historyentity.*;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.JobNameNormalization;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A new instance of JHFEventReaderBase will be created for each job history log file.
 */
public abstract class JHFEventReaderBase extends JobEntityCreationPublisher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JHFEventReaderBase.class);
    protected Map<String, String> baseTags;
    protected JobEventAPIEntity jobSubmitEventEntity;
    protected JobEventAPIEntity jobLaunchEventEntity;
    protected int numTotalMaps;
    protected int numTotalReduces;
    protected JobEventAPIEntity jobFinishEventEntity;
    protected JobExecutionAPIEntity jobExecutionEntity;
    protected Map<String, Long> taskStartTime;
    // taskAttemptID to task attempt startTime
    protected Map<String, Long> taskAttemptStartTime;

    // taskID to host mapping, for task it's the host where the last attempt runs on
    protected Map<String, String> taskRunningHosts;
    // hostname to rack mapping
    protected Map<String, String> host2RackMapping;

    protected String jobId;
    protected String jobName;
    protected String jobType;
    protected String jobDefId;
    protected String user;
    protected String queueName;
    protected Long jobLaunchTime;
    protected JobHistoryContentFilter filter;

    protected final List<HistoryJobEntityLifecycleListener> jobEntityLifecycleListeners = new ArrayList<>();

    protected final Configuration configuration;

    private long sumMapTaskDuration;
    private long sumReduceTaskDuration;

    private JobCounterMetricsGenerator jobCounterMetricsGenerator;

    /**
     * baseTags stores the basic tag name values which might be used for persisting various entities.
     * baseTags includes: site and jobName
     * baseTags are used for all job/task related entities
     *
     * @param baseTags
     */
    public JHFEventReaderBase(Map<String, String> baseTags, Configuration configuration, JobHistoryContentFilter filter) {
        this.filter = filter;

        this.baseTags = baseTags;
        jobSubmitEventEntity = new JobEventAPIEntity();
        jobSubmitEventEntity.setTags(new HashMap<>(baseTags));

        jobLaunchEventEntity = new JobEventAPIEntity();
        jobLaunchEventEntity.setTags(new HashMap<>(baseTags));

        jobFinishEventEntity = new JobEventAPIEntity();
        jobFinishEventEntity.setTags(new HashMap<>(baseTags));

        jobExecutionEntity = new JobExecutionAPIEntity();
        jobExecutionEntity.setTags(new HashMap<>(baseTags));
        jobExecutionEntity.setNumFailedMaps(0);
        jobExecutionEntity.setNumFailedReduces(0);
        jobExecutionEntity.setFailedTasks(new HashMap<>());

        taskRunningHosts = new HashMap<>();

        host2RackMapping = new HashMap<>();

        taskStartTime = new HashMap<>();
        taskAttemptStartTime = new HashMap<>();

        this.configuration = configuration;

        if (this.configuration != null && this.jobType == null) {
            this.setJobType(Utils.fetchJobType(this.configuration).toString());
        }
        this.sumMapTaskDuration = 0L;
        this.sumReduceTaskDuration = 0L;
        this.jobCounterMetricsGenerator = new JobCounterMetricsGenerator();
    }

    public void register(HistoryJobEntityLifecycleListener lifecycleListener) {
        this.jobEntityLifecycleListeners.add(lifecycleListener);
    }

    @Override
    public void close() throws IOException {
        // check if this job history file is complete
        if (jobExecutionEntity.getEndTime() == 0L) {
            throw new IOException(new JHFWriteNotCompletedException(jobId));
        }
        try {
            flush();
            this.jobCounterMetricsGenerator.flush();
        } catch (Exception ex) {
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

    private String buildJobTrackingUrl(String jobId) {
        String jobTrackingUrlBase = MRHistoryJobConfig.getInstance(null).getJobHistoryEndpointConfig().mrHistoryServerUrl + "/jobhistory/job/";
        try {
            URI oldUri = new URI(jobTrackingUrlBase);
            URI resolved = oldUri.resolve(jobId);
            return resolved.toString();
        } catch (URISyntaxException e) {
            LOG.warn("Tracking url build failed with baseURL=%s, resolvePart=%s", jobTrackingUrlBase, jobId);
            return jobTrackingUrlBase;
        }
    }

    /**
     * ...
     * @param id
     */
    private void setJobID(String id) {
        this.jobId = id;
    }

    private void setJobType(String jobType) {
        this.jobType = jobType;
    }

    protected void handleJob(EventType eventType, Map<Keys, String> values, Object totalCounters) throws Exception {
        String id = values.get(Keys.JOBID);

        if (jobId == null) {
            setJobID(id);
        } else if (!jobId.equals(id)) {
            String msg = "Current job ID '" + id + "' does not match previously stored value '" + jobId + "'";
            LOG.error(msg);
            throw new ImportException(msg);
        }

        if (values.get(Keys.SUBMIT_TIME) != null) {  // job submitted
            jobSubmitEventEntity.setTimestamp(Long.valueOf(values.get(Keys.SUBMIT_TIME)));
            user = values.get(Keys.USER);
            queueName = values.get(Keys.JOB_QUEUE);
            jobName = values.get(Keys.JOBNAME);

            // If given job name then use it as norm job name, otherwise use eagle JobNameNormalization rule to generate.
            String jobDefId = null;
            if (configuration != null) {
                jobDefId = configuration.get(filter.getJobNameKey());
            }

            if (jobDefId == null) {
                this.jobDefId = JobNameNormalization.getInstance().normalize(jobName);
            } else {
                LOG.debug("Got JobDefId from job configuration for " + id + ": " + jobDefId);
                this.jobDefId = jobDefId;
            }

            LOG.info("JobDefId of " + id + ": " + this.jobDefId);

            jobSubmitEventEntity.getTags().put(MRJobTagName.USER.toString(), user);
            jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), jobId);
            jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), EagleJobStatus.SUBMITTED.name());
            jobSubmitEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), jobName);
            jobSubmitEventEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), this.jobDefId);
            jobExecutionEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), this.jobType);
            entityCreated(jobSubmitEventEntity);
        } else if (values.get(Keys.LAUNCH_TIME) != null) {  // job launched
            jobLaunchEventEntity.setTimestamp(Long.valueOf(values.get(Keys.LAUNCH_TIME)));
            jobLaunchTime = jobLaunchEventEntity.getTimestamp();
            jobLaunchEventEntity.getTags().put(MRJobTagName.USER.toString(), user);
            jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), jobId);
            jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), EagleJobStatus.LAUNCHED.name());
            jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), jobName);
            jobLaunchEventEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
            jobLaunchEventEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), this.jobType);
            numTotalMaps = Integer.valueOf(values.get(Keys.TOTAL_MAPS));
            numTotalReduces = Integer.valueOf(values.get(Keys.TOTAL_REDUCES));
            entityCreated(jobLaunchEventEntity);
        } else if (values.get(Keys.FINISH_TIME) != null) {  // job finished
            jobFinishEventEntity.setTimestamp(Long.valueOf(values.get(Keys.FINISH_TIME)));
            jobFinishEventEntity.getTags().put(MRJobTagName.USER.toString(), user);
            jobFinishEventEntity.getTags().put(MRJobTagName.JOB_ID.toString(), jobId);
            jobFinishEventEntity.getTags().put(MRJobTagName.JOB_STATUS.toString(), values.get(Keys.JOB_STATUS));
            jobFinishEventEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), jobName);
            jobFinishEventEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
            jobFinishEventEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), this.jobType);
            entityCreated(jobFinishEventEntity);

            // populate jobExecutionEntity entity
            jobExecutionEntity.getTags().put(MRJobTagName.USER.toString(), user);
            jobExecutionEntity.getTags().put(MRJobTagName.JOB_ID.toString(), jobId);
            jobExecutionEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), jobName);
            jobExecutionEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
            jobExecutionEntity.getTags().put(MRJobTagName.JOB_QUEUE.toString(), queueName);
            jobExecutionEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), this.jobType);

            jobExecutionEntity.setTrackingUrl(buildJobTrackingUrl(jobId));
            jobExecutionEntity.setCurrentState(values.get(Keys.JOB_STATUS));
            jobExecutionEntity.setStartTime(jobLaunchEventEntity.getTimestamp());
            jobExecutionEntity.setEndTime(jobFinishEventEntity.getTimestamp());
            jobExecutionEntity.setDurationTime(jobExecutionEntity.getEndTime() - jobExecutionEntity.getStartTime());
            jobExecutionEntity.setTimestamp(jobLaunchEventEntity.getTimestamp());
            jobExecutionEntity.setSubmissionTime(jobSubmitEventEntity.getTimestamp());
            if (values.get(Keys.FAILED_MAPS) != null) {
                // for Artemis
                jobExecutionEntity.setNumFailedMaps(Integer.valueOf(values.get(Keys.FAILED_MAPS)));
            }
            if (values.get(Keys.FAILED_REDUCES) != null) {
                // for Artemis
                jobExecutionEntity.setNumFailedReduces(Integer.valueOf(values.get(Keys.FAILED_REDUCES)));
            }
            jobExecutionEntity.setNumFinishedMaps(Integer.valueOf(values.get(Keys.FINISHED_MAPS)));
            jobExecutionEntity.setNumFinishedReduces(Integer.valueOf(values.get(Keys.FINISHED_REDUCES)));
            jobExecutionEntity.setNumTotalMaps(numTotalMaps);
            jobExecutionEntity.setNumTotalReduces(numTotalReduces);
            if (values.get(Keys.COUNTERS) != null || totalCounters != null) {
                JobCounters jobCounters = parseCounters(totalCounters);
                jobExecutionEntity.setJobCounters(jobCounters);
                if (jobCounters.getCounters().containsKey(Constants.JOB_COUNTER)) {
                    Map<String, Long> counters = jobCounters.getCounters().get(Constants.JOB_COUNTER);
                    if (counters.containsKey(Constants.JobCounter.DATA_LOCAL_MAPS.toString())) {
                        jobExecutionEntity.setDataLocalMaps(counters.get(Constants.JobCounter.DATA_LOCAL_MAPS.toString()).intValue());
                    }

                    if (counters.containsKey(Constants.JobCounter.RACK_LOCAL_MAPS.toString())) {
                        jobExecutionEntity.setRackLocalMaps(counters.get(Constants.JobCounter.RACK_LOCAL_MAPS.toString()).intValue());
                    }

                    if (counters.containsKey(Constants.JobCounter.TOTAL_LAUNCHED_MAPS.toString())) {
                        jobExecutionEntity.setTotalLaunchedMaps(counters.get(Constants.JobCounter.TOTAL_LAUNCHED_MAPS.toString()).intValue());
                    }
                }

                if (jobExecutionEntity.getTotalLaunchedMaps() > 0) {
                    jobExecutionEntity.setDataLocalMapsPercentage(jobExecutionEntity.getDataLocalMaps() * 1.0 / jobExecutionEntity.getTotalLaunchedMaps());
                    jobExecutionEntity.setRackLocalMapsPercentage(jobExecutionEntity.getRackLocalMaps() * 1.0 / jobExecutionEntity.getTotalLaunchedMaps());
                }
            }
            jobExecutionEntity.setAvgMapTaskDuration(this.sumMapTaskDuration * 1.0 / numTotalMaps);
            if (numTotalReduces == 0) {
                jobExecutionEntity.setMaxReduceTaskDuration(0);
                jobExecutionEntity.setAvgReduceTaskDuration(0);
            } else {
                jobExecutionEntity.setAvgReduceTaskDuration(this.sumReduceTaskDuration * 1.0 / numTotalReduces);
            }
            this.jobCounterMetricsGenerator.setBaseTags(jobExecutionEntity.getTags());
            entityCreated(jobExecutionEntity);
        }
    }

    private void entityCreated(JobBaseAPIEntity entity) throws Exception {
        for (HistoryJobEntityLifecycleListener lifecycleListener : this.jobEntityLifecycleListeners) {
            lifecycleListener.jobEntityCreated(entity);
        }

        // job finished when passing JobExecutionAPIEntity: jobExecutionEntity
        if (entity == this.jobExecutionEntity) {
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
     *
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

        Map<String, String> taskBaseTags = new HashMap<String, String>() {
            {
                put(MRJobTagName.TASK_TYPE.toString(), taskType);
                put(MRJobTagName.USER.toString(), user);
                //put(MRJobTagName.JOB_NAME.toString(), _jobName);
                put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
                put(MRJobTagName.JOB_TYPE.toString(), jobType);
                put(MRJobTagName.JOB_ID.toString(), jobId);
                put(MRJobTagName.TASK_ID.toString(), taskID);
            }
        };
        taskBaseTags.putAll(baseTags);
        if (recType == RecordTypes.Task && startTime != null) { // task start, no host is assigned yet
            taskStartTime.put(taskID, Long.valueOf(startTime));
        } else if (recType == RecordTypes.Task && finishTime != null) { // task finish
            // task execution entity setup
            Map<String, String> taskExecutionTags = new HashMap<>(taskBaseTags);
            String hostname = taskRunningHosts.get(taskID);
            hostname = (hostname == null) ? "" : hostname; // TODO if task fails, then no hostname
            taskExecutionTags.put(MRJobTagName.HOSTNAME.toString(), hostname);
            taskExecutionTags.put(MRJobTagName.RACK.toString(), host2RackMapping.get(hostname));
            TaskExecutionAPIEntity entity = new TaskExecutionAPIEntity();
            entity.setTags(taskExecutionTags);
            entity.setStartTime(taskStartTime.get(taskID));
            entity.setEndTime(Long.valueOf(finishTime));
            entity.setDuration(entity.getEndTime() - entity.getStartTime());
            entity.setTimestamp(jobLaunchTime);
            entity.setError(values.get(Keys.ERROR));
            entity.setTaskStatus(values.get(Keys.TASK_STATUS));
            if (values.get(Keys.COUNTERS) != null || counters != null) {
                entity.setJobCounters(parseCounters(counters));
            }
            long duration = entity.getEndTime() - jobLaunchTime;
            if (taskType.equals(Constants.TaskType.MAP.toString()) && duration > jobExecutionEntity.getLastMapDuration()) {
                jobExecutionEntity.setLastMapDuration(duration);
            }
            if (taskType.equals(Constants.TaskType.REDUCE.toString()) && duration > jobExecutionEntity.getLastReduceDuration()) {
                jobExecutionEntity.setLastReduceDuration(duration);
            }

            if (taskType.equals(Constants.TaskType.MAP.toString()) && entity.getDuration() > jobExecutionEntity.getMaxMapTaskDuration()) {
                jobExecutionEntity.setMaxMapTaskDuration(entity.getDuration());
            }
            if (taskType.equals(Constants.TaskType.REDUCE.toString()) && entity.getDuration() > jobExecutionEntity.getMaxReduceTaskDuration()) {
                jobExecutionEntity.setMaxReduceTaskDuration(entity.getDuration());
            }

            if (taskType.equals(Constants.TaskType.MAP.toString())) {
                this.sumMapTaskDuration += entity.getDuration();
                if (entity.getTaskStatus().equals(EagleTaskStatus.FAILED.name())
                    || entity.getTaskStatus().equals(EagleTaskStatus.KILLED.name())) {
                    jobExecutionEntity.setNumFailedMaps(1 + jobExecutionEntity.getNumFailedMaps());
                }
            } else {
                this.sumReduceTaskDuration += entity.getDuration();
                if (entity.getTaskStatus().equals(EagleTaskStatus.FAILED.name())
                    || entity.getTaskStatus().equals(EagleTaskStatus.KILLED.name())) {
                    jobExecutionEntity.setNumFailedReduces(1 + jobExecutionEntity.getNumFailedReduces());
                }
            }

            entityCreated(entity);
            this.jobCounterMetricsGenerator.taskExecutionEntityCreated(entity);
            //_taskStartTime.remove(taskID); // clean this taskID
        } else if ((recType == RecordTypes.MapAttempt || recType == RecordTypes.ReduceAttempt) && startTime != null) { // task attempt start
            taskAttemptStartTime.put(taskAttemptID, Long.valueOf(startTime));
        } else if ((recType == RecordTypes.MapAttempt || recType == RecordTypes.ReduceAttempt) && finishTime != null) {   // task attempt finish
            TaskAttemptExecutionAPIEntity entity = new TaskAttemptExecutionAPIEntity();
            Map<String, String> taskAttemptExecutionTags = new HashMap<>(taskBaseTags);
            entity.setTags(taskAttemptExecutionTags);
            String hostname = values.get(Keys.HOSTNAME);
            String rack = values.get(Keys.RACK);
            taskAttemptExecutionTags.put(MRJobTagName.HOSTNAME.toString(), hostname);
            taskAttemptExecutionTags.put(MRJobTagName.RACK.toString(), rack);
            // put last attempt's hostname to task level
            taskRunningHosts.put(taskID, hostname);
            // it is very likely that an attempt ID could be both succeeded and failed due to M/R system
            // in this case, we should ignore this attempt?
            if (taskAttemptStartTime.get(taskAttemptID) == null) {
                LOG.warn("task attemp has consistency issue " + taskAttemptID);
                return;
            }
            entity.setStartTime(taskAttemptStartTime.get(taskAttemptID));
            entity.setEndTime(Long.valueOf(finishTime));
            entity.setTimestamp(jobLaunchTime);
            entity.setDuration(entity.getEndTime() - entity.getStartTime());
            entity.setTaskStatus(values.get(Keys.TASK_STATUS));
            entity.setError(values.get(Keys.ERROR));
            if (values.get(Keys.COUNTERS) != null || counters != null) {  // when task is killed, COUNTERS does not exist
                //entity.setJobCounters(parseCounters(values.get(Keys.COUNTERS)));
                entity.setJobCounters(parseCounters(counters));
            }
            entity.setTaskAttemptID(taskAttemptID);

            if (recType == RecordTypes.MapAttempt) {
                jobExecutionEntity.setTotalMapAttempts(1 + jobExecutionEntity.getTotalMapAttempts());
                if (entity.getTaskStatus().equals(EagleTaskStatus.FAILED.name())
                    || entity.getTaskStatus().equals(EagleTaskStatus.KILLED.name())) {
                    jobExecutionEntity.setFailedMapAttempts(1 + jobExecutionEntity.getFailedMapAttempts());
                }
            } else {
                jobExecutionEntity.setTotalReduceAttempts(1 + jobExecutionEntity.getTotalReduceAttempts());
                if (entity.getTaskStatus().equals(EagleTaskStatus.FAILED.name())
                    || entity.getTaskStatus().equals(EagleTaskStatus.KILLED.name())) {
                    jobExecutionEntity.setFailedReduceAttempts(1 + jobExecutionEntity.getFailedReduceAttempts());
                }
            }

            entityCreated(entity);
            if (entity.getTags().get(MRJobTagName.ERROR_CATEGORY.toString()) != null) {
                jobExecutionEntity.getFailedTasks().put(taskID,
                    new HashMap<String, String>() {
                        {
                            put(entity.getTags().get(MRJobTagName.ERROR_CATEGORY.toString()),
                                entity.getTags().get(MRJobTagName.ERROR_CATEGORY.toString()));//decide later
                        }
                    }
                );
            }
            taskAttemptStartTime.remove(taskAttemptID);
        } else {
            // silently ignore
            LOG.warn("It's an exceptional case ?");
        }
    }

    public void parseConfiguration() throws Exception {
        Map<String, String> prop = new TreeMap<>();

        if (filter.acceptJobConfFile()) {
            Iterator<Map.Entry<String, String>> iter = configuration.iterator();
            while (iter.hasNext()) {
                String key = iter.next().getKey();
                if (included(key) && !excluded(key)) {
                    prop.put(key, configuration.get(key));
                }
            }
        }

        // check must-have keys are within prop
        if (matchMustHaveKeyPatterns(prop)) {
            JobConfigurationAPIEntity jobConfigurationEntity = new JobConfigurationAPIEntity();
            jobConfigurationEntity.setTags(new HashMap<>(baseTags));
            jobConfigurationEntity.getTags().put(MRJobTagName.USER.toString(), user);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_ID.toString(), jobId);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), jobName);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
            jobConfigurationEntity.getTags().put(MRJobTagName.JOB_TYPE.toString(), jobType);
            jobConfigurationEntity.setTimestamp(jobLaunchEventEntity.getTimestamp());

            JobConfig jobConfig = new JobConfig();
            jobConfig.setConfig(prop);
            jobConfigurationEntity.setJobConfig(jobConfig);
            jobConfigurationEntity.setConfigJobName(jobDefId);
            entityCreated(jobConfigurationEntity);
        }
    }

    private boolean matchMustHaveKeyPatterns(Map<String, String> prop) {
        if (filter.getMustHaveJobConfKeyPatterns() == null) {
            return true;
        }

        for (Pattern p : filter.getMustHaveJobConfKeyPatterns()) {
            boolean matched = false;
            for (String key : prop.keySet()) {
                if (p.matcher(key).matches()) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private boolean included(String key) {
        if (filter.getJobConfKeyInclusionPatterns() == null) {
            return true;
        }
        for (Pattern p : filter.getJobConfKeyInclusionPatterns()) {
            Matcher m = p.matcher(key);
            if (m.matches()) {
                LOG.info("include key: " + p.toString());
                return true;
            }
        }
        return false;
    }

    private boolean excluded(String key) {
        if (filter.getJobConfKeyExclusionPatterns() == null) {
            return false;
        }
        for (Pattern p : filter.getJobConfKeyExclusionPatterns()) {
            Matcher m = p.matcher(key);
            if (m.matches()) {
                return true;
            }
        }
        return false;
    }
}