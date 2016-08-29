/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.spark.crawl;

import org.apache.commons.lang.ArrayUtils;
import org.apache.eagle.jpm.spark.entity.*;
import org.apache.eagle.jpm.util.*;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.impl.EagleServiceBaseClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JHFSparkEventReader {
    private static final Logger LOG = LoggerFactory.getLogger(JHFSparkEventReader.class);

    private static final int FLUSH_LIMIT = 500;
    private long firstTaskLaunchTime;
    private long lastEventTime;

    private Map<String, SparkExecutor> executors;
    private SparkApp app;
    private Map<Integer, SparkJob> jobs;
    private Map<String, SparkStage> stages;
    private Map<Integer, Set<String>> jobStageMap;
    private Map<Integer, SparkTask> tasks;
    private EagleServiceClientImpl client;
    private Map<String, Map<Integer, Boolean>> stageTaskStatusMap;

    private List<TaggedLogAPIEntity> createEntities;

    private Config conf;

    public JHFSparkEventReader(Map<String, String> baseTags, SparkApplicationInfo info) {
        app = new SparkApp();
        app.setTags(new HashMap<String, String>(baseTags));
        app.setYarnState(info.getState());
        app.setYarnStatus(info.getFinalStatus());
        createEntities = new ArrayList<>();
        jobs = new HashMap<Integer, SparkJob>();
        stages = new HashMap<String, SparkStage>();
        jobStageMap = new HashMap<Integer, Set<String>>();
        tasks = new HashMap<Integer, SparkTask>();
        executors = new HashMap<String, SparkExecutor>();
        stageTaskStatusMap = new HashMap<>();
        conf = ConfigFactory.load();
        this.initiateClient();
    }

    public SparkApp getApp() {
        return this.app;
    }

    public void read(JSONObject eventObj) throws Exception {
        String eventType = (String) eventObj.get("Event");
        if (eventType.equalsIgnoreCase(EventType.SparkListenerApplicationStart.toString())) {
            handleAppStarted(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerEnvironmentUpdate.toString())) {
            handleEnvironmentSet(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerExecutorAdded.toString())) {
            handleExecutorAdd(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerBlockManagerAdded.toString())) {
            handleBlockManagerAdd(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerJobStart.toString())) {
            handleJobStart(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerStageSubmitted.toString())) {
            handleStageSubmit(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerTaskStart.toString())) {
            handleTaskStart(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerTaskEnd.toString())) {
            handleTaskEnd(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerStageCompleted.toString())) {
            handleStageComplete(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerJobEnd.toString())) {
            handleJobEnd(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerExecutorRemoved.toString())) {
            handleExecutorRemoved(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerApplicationEnd.toString())) {
            handleAppEnd(eventObj);
        } else if (eventType.equalsIgnoreCase(EventType.SparkListenerBlockManagerRemoved.toString())) {
            //nothing to do now
        } else {
            LOG.info("Not registered event type:" + eventType);
        }

    }

    private void handleEnvironmentSet(JSONObject event) {
        app.setConfig(new JobConfig());
        JSONObject sparkProps = (JSONObject) event.get("Spark Properties");

        String[] additionalJobConf = conf.getString("basic.jobConf.additional.info").split(",\\s*");
        String[] props = {"spark.yarn.app.id", "spark.executor.memory", "spark.driver.host", "spark.driver.port",
            "spark.driver.memory", "spark.scheduler.pool", "spark.executor.cores", "spark.yarn.am.memory",
            "spark.yarn.am.cores", "spark.yarn.executor.memoryOverhead", "spark.yarn.driver.memoryOverhead", "spark.yarn.am.memoryOverhead", "spark.master"};
        String[] jobConf = (String[])ArrayUtils.addAll(additionalJobConf, props);
        for (String prop : jobConf) {
            if (sparkProps.containsKey(prop)) {
                app.getConfig().getConfig().put(prop, (String) sparkProps.get(prop));
            }
        }
    }

    private Object getConfigVal(JobConfig config, String configName, String type) {
        if (config.getConfig().containsKey(configName)) {
            Object val = config.getConfig().get(configName);
            if (type.equalsIgnoreCase(Integer.class.getName())) {
                return Integer.parseInt((String) val);
            } else {
                return val;
            }
        } else {
            if (type.equalsIgnoreCase(Integer.class.getName())) {
                return conf.getInt("spark.defaultVal." + configName);
            } else {
                return conf.getString("spark.defaultVal." + configName);
            }
        }
    }

    private boolean isClientMode(JobConfig config) {
        return config.getConfig().get("spark.master").equalsIgnoreCase("yarn-client");
    }

    private void handleAppStarted(JSONObject event) {
        //need update all entities tag before app start
        List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
        entities.addAll(this.executors.values());
        entities.add(this.app);

        long appStartTime = JSONUtils.getLong(event, "Timestamp", lastEventTime);
        for (TaggedLogAPIEntity entity : entities) {
            entity.getTags().put(SparkJobTagName.SPARK_APP_ID.toString(), JSONUtils.getString(event, "App ID"));
            entity.getTags().put(SparkJobTagName.SPARK_APP_NAME.toString(), JSONUtils.getString(event, "App Name"));
            // In yarn-client mode, attemptId is not available in the log, so we set attemptId = 1.
            String attemptId = isClientMode(this.app.getConfig()) ? "1" : JSONUtils.getString(event, "App Attempt ID");
            entity.getTags().put(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString(), attemptId);
            // the second argument of getNormalizeName() is changed to null because the original code contains sensitive text
            // original second argument looks like: this.app.getConfig().getConfig().get("xxx"), "xxx" is the sensitive text
            entity.getTags().put(SparkJobTagName.SPARK_APP_NORM_NAME.toString(), this.getNormalizedName(JSONUtils.getString(event, "App Name"), null));
            entity.getTags().put(SparkJobTagName.SPARK_USER.toString(), JSONUtils.getString(event, "User"));

            entity.setTimestamp(appStartTime);
        }

        this.app.setStartTime(appStartTime);
        this.lastEventTime = appStartTime;
    }

    private void handleExecutorAdd(JSONObject event) throws Exception {
        String executorID = (String) event.get("Executor ID");
        long executorAddTime = JSONUtils.getLong(event, "Timestamp", lastEventTime);
        this.lastEventTime = executorAddTime;
        SparkExecutor executor = this.initiateExecutor(executorID, executorAddTime);

        JSONObject executorInfo = JSONUtils.getJSONObject(event, "Executor Info");

    }

    private void handleBlockManagerAdd(JSONObject event) throws Exception {
        long maxMemory = JSONUtils.getLong(event, "Maximum Memory");
        long timestamp = JSONUtils.getLong(event, "Timestamp", lastEventTime);
        this.lastEventTime = timestamp;
        JSONObject blockInfo = JSONUtils.getJSONObject(event, "Block Manager ID");
        String executorID = JSONUtils.getString(blockInfo, "Executor ID");
        String hostAndPort = JSONUtils.getString(blockInfo, "Host") + ":" + JSONUtils.getLong(blockInfo, "Port");

        SparkExecutor executor = this.initiateExecutor(executorID, timestamp);
        executor.setMaxMemory(maxMemory);
        executor.setHostPort(hostAndPort);
    }

    private void handleTaskStart(JSONObject event) {
        this.initializeTask(event);
    }

    private void handleTaskEnd(JSONObject event) {
        JSONObject taskInfo = JSONUtils.getJSONObject(event, "Task Info");
        int taskId = JSONUtils.getInt(taskInfo, "Task ID");
        SparkTask task = tasks.get(taskId);
        if (task == null) {
            return;
        }

        task.setFailed(JSONUtils.getBoolean(taskInfo, "Failed"));
        JSONObject taskMetrics = JSONUtils.getJSONObject(event, "Task Metrics");
        if (null != taskMetrics) {
            task.setExecutorDeserializeTime(JSONUtils.getLong(taskMetrics, "Executor Deserialize Time", lastEventTime));
            task.setExecutorRunTime(JSONUtils.getLong(taskMetrics, "Executor Run Time", lastEventTime));
            task.setJvmGcTime(JSONUtils.getLong(taskMetrics, "JVM GC Time", lastEventTime));
            task.setResultSize(JSONUtils.getLong(taskMetrics, "Result Size"));
            task.setResultSerializationTime(JSONUtils.getLong(taskMetrics, "Result Serialization Time", lastEventTime));
            task.setMemoryBytesSpilled(JSONUtils.getLong(taskMetrics, "Memory Bytes Spilled"));
            task.setDiskBytesSpilled(JSONUtils.getLong(taskMetrics, "Disk Bytes Spilled"));

            JSONObject inputMetrics = JSONUtils.getJSONObject(taskMetrics, "Input Metrics");
            if (null != inputMetrics) {
                task.setInputBytes(JSONUtils.getLong(inputMetrics, "Bytes Read"));
                task.setInputRecords(JSONUtils.getLong(inputMetrics, "Records Read"));
            }

            JSONObject outputMetrics = JSONUtils.getJSONObject(taskMetrics, "Output Metrics");
            if (null != outputMetrics) {
                task.setOutputBytes(JSONUtils.getLong(outputMetrics, "Bytes Written"));
                task.setOutputRecords(JSONUtils.getLong(outputMetrics, "Records Written"));
            }

            JSONObject shuffleWriteMetrics = JSONUtils.getJSONObject(taskMetrics, "Shuffle Write Metrics");
            if (null != shuffleWriteMetrics) {
                task.setShuffleWriteBytes(JSONUtils.getLong(shuffleWriteMetrics, "Shuffle Bytes Written"));
                task.setShuffleWriteRecords(JSONUtils.getLong(shuffleWriteMetrics, "Shuffle Records Written"));
            }

            JSONObject shuffleReadMetrics = JSONUtils.getJSONObject(taskMetrics, "Shuffle Read Metrics");
            if (null != shuffleReadMetrics) {
                task.setShuffleReadLocalBytes(JSONUtils.getLong(shuffleReadMetrics, "Local Bytes Read"));
                task.setShuffleReadRemoteBytes(JSONUtils.getLong(shuffleReadMetrics, "Remote Bytes Read"));
                task.setShuffleReadRecords(JSONUtils.getLong(shuffleReadMetrics, "Total Records Read"));
            }
        } else {
            //for tasks success without task metrics, save in the end if no other information
            if (!task.isFailed()) {
                return;
            }
        }

        aggregateToStage(task);
        aggregateToExecutor(task);
        tasks.remove(taskId);
        this.flushEntities(task, false);
    }


    private SparkTask initializeTask(JSONObject event) {
        SparkTask task = new SparkTask();
        task.setTags(new HashMap<>(this.app.getTags()));
        task.setTimestamp(app.getTimestamp());

        task.getTags().put(SparkJobTagName.SPARK_SATGE_ID.toString(), Long.toString(JSONUtils.getLong(event, "Stage ID")));
        task.getTags().put(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString(), Long.toString(JSONUtils.getLong(event, "Stage Attempt ID")));

        JSONObject taskInfo = JSONUtils.getJSONObject(event, "Task Info");
        int taskId = JSONUtils.getInt(taskInfo, "Task ID");
        task.setTaskId(taskId);

        task.getTags().put(SparkJobTagName.SPARK_TASK_INDEX.toString(), Integer.toString(JSONUtils.getInt(taskInfo, "Index")));
        task.getTags().put(SparkJobTagName.SPARK_TASK_ATTEMPT_ID.toString(), Integer.toString(JSONUtils.getInt(taskInfo, "Attempt")));
        long launchTime = JSONUtils.getLong(taskInfo, "Launch Time", lastEventTime);
        this.lastEventTime = launchTime;
        if (taskId == 0) {
            this.setFirstTaskLaunchTime(launchTime);
        }
        task.setLaunchTime(launchTime);
        task.setExecutorId(JSONUtils.getString(taskInfo, "Executor ID"));
        task.setHost(JSONUtils.getString(taskInfo, "Host"));
        task.setTaskLocality(JSONUtils.getString(taskInfo, "Locality"));
        task.setSpeculative(JSONUtils.getBoolean(taskInfo, "Speculative"));

        tasks.put(task.getTaskId(), task);
        return task;
    }

    private void setFirstTaskLaunchTime(long launchTime) {
        this.firstTaskLaunchTime = launchTime;
    }

    private void handleJobStart(JSONObject event) {
        SparkJob job = new SparkJob();
        job.setTags(new HashMap<>(this.app.getTags()));
        job.setTimestamp(app.getTimestamp());

        int jobId = JSONUtils.getInt(event, "Job ID");
        job.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), Integer.toString(jobId));
        long submissionTime = JSONUtils.getLong(event, "Submission Time", lastEventTime);
        job.setSubmissionTime(submissionTime);
        this.lastEventTime = submissionTime;

        //for complete application, no active stages/tasks
        job.setNumActiveStages(0);
        job.setNumActiveTasks(0);

        this.jobs.put(jobId, job);
        this.jobStageMap.put(jobId, new HashSet<String>());

        JSONArray stages = JSONUtils.getJSONArray(event, "Stage Infos");
        int stagesSize = (stages == null ? 0 : stages.size());
        job.setNumStages(stagesSize);
        for (int i = 0; i < stagesSize; i++) {
            JSONObject stageInfo = (JSONObject) stages.get(i);
            int stageId = JSONUtils.getInt(stageInfo, "Stage ID");
            int stageAttemptId = JSONUtils.getInt(stageInfo, "Stage Attempt ID");
            String stageName = JSONUtils.getString(stageInfo, "Stage Name");
            int numTasks = JSONUtils.getInt(stageInfo, "Number of Tasks");
            this.initiateStage(jobId, stageId, stageAttemptId, stageName, numTasks);
        }
    }

    private void handleStageSubmit(JSONObject event) {
        JSONObject stageInfo = JSONUtils.getJSONObject(event, "Stage Info");
        int stageId = JSONUtils.getInt(stageInfo, "Stage ID");
        int stageAttemptId = JSONUtils.getInt(stageInfo, "Stage Attempt ID");
        String key = this.generateStageKey(Integer.toString(stageId), Integer.toString(stageAttemptId));
        stageTaskStatusMap.put(key, new HashMap<Integer, Boolean>());

        if (!stages.containsKey(this.generateStageKey(Integer.toString(stageId), Integer.toString(stageAttemptId)))) {
            //may be further attempt for one stage
            String baseAttempt = this.generateStageKey(Integer.toString(stageId), "0");
            if (stages.containsKey(baseAttempt)) {
                SparkStage stage = stages.get(baseAttempt);
                String jobId = stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString());

                String stageName = JSONUtils.getString(event, "Stage Name");
                int numTasks = JSONUtils.getInt(stageInfo, "Number of Tasks");
                this.initiateStage(Integer.parseInt(jobId), stageId, stageAttemptId, stageName, numTasks);
            }
        }
    }

    private void handleStageComplete(JSONObject event) {
        JSONObject stageInfo = JSONUtils.getJSONObject(event, "Stage Info");
        int stageId = JSONUtils.getInt(stageInfo, "Stage ID");
        int stageAttemptId = JSONUtils.getInt(stageInfo, "Stage Attempt ID");
        String key = this.generateStageKey(Integer.toString(stageId), Integer.toString(stageAttemptId));
        SparkStage stage = stages.get(key);

        // If "Submission Time" is not available, use the "Launch Time" of "Task ID" = 0.
        Long submissionTime = JSONUtils.getLong(stageInfo, "Submission Time", firstTaskLaunchTime);

        stage.setSubmitTime(submissionTime);

        long completeTime = JSONUtils.getLong(stageInfo, "Completion Time", lastEventTime);
        stage.setCompleteTime(completeTime);
        this.lastEventTime = completeTime;

        if (stageInfo.containsKey("Failure Reason")) {
            stage.setStatus(SparkEntityConstant.SparkStageStatus.FAILED.toString());
        } else {
            stage.setStatus(SparkEntityConstant.SparkStageStatus.COMPLETE.toString());
        }
    }

    private void handleExecutorRemoved(JSONObject event) {
        String executorID = JSONUtils.getString(event, "Executor ID");
        SparkExecutor executor = executors.get(executorID);
        long removedTime = JSONUtils.getLong(event, "Timestamp", lastEventTime);
        executor.setEndTime(removedTime);
        this.lastEventTime = removedTime;
    }

    private void handleJobEnd(JSONObject event) {
        int jobId = JSONUtils.getInt(event, "Job ID");
        SparkJob job = jobs.get(jobId);

        long completionTime = JSONUtils.getLong(event, "Completion Time", lastEventTime);
        job.setCompletionTime(completionTime);
        this.lastEventTime = completionTime;

        JSONObject jobResult = JSONUtils.getJSONObject(event, "Job Result");
        String result = JSONUtils.getString(jobResult, "Result");
        if (result.equalsIgnoreCase("JobSucceeded")) {
            job.setStatus(SparkEntityConstant.SparkJobStatus.SUCCEEDED.toString());
        } else {
            job.setStatus(SparkEntityConstant.SparkJobStatus.FAILED.toString());
        }
    }

    private void handleAppEnd(JSONObject event) {
        long endTime = JSONUtils.getLong(event, "Timestamp", lastEventTime);
        app.setEndTime(endTime);
        this.lastEventTime = endTime;
    }

    public void clearReader() throws Exception {
        //clear tasks
        for (SparkTask task : tasks.values()) {
            LOG.info("Task {} does not have result or no task metrics.", task.getTaskId());
            task.setFailed(true);
            aggregateToStage(task);
            aggregateToExecutor(task);
            this.flushEntities(task, false);
        }

        List<SparkStage> needStoreStages = new ArrayList<>();
        for (SparkStage stage : this.stages.values()) {
            int jobId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString()));
            if (stage.getSubmitTime() == 0 || stage.getCompleteTime() == 0) {
                SparkJob job = this.jobs.get(jobId);
                job.setNumSkippedStages(job.getNumSkippedStages() + 1);
                job.setNumSkippedTasks(job.getNumSkippedTasks() + stage.getNumTasks());
            } else {
                this.aggregateToJob(stage);
                this.aggregateStageToApp(stage);
                needStoreStages.add(stage);
            }
            String stageId = stage.getTags().get(SparkJobTagName.SPARK_SATGE_ID.toString());
            String stageAttemptId = stage.getTags().get(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString());
            this.jobStageMap.get(jobId).remove(this.generateStageKey(stageId, stageAttemptId));
        }

        this.flushEntities(needStoreStages, false);
        for (SparkJob job : jobs.values()) {
            this.aggregateJobToApp(job);
        }
        this.flushEntities(jobs.values(), false);

        app.setExecutors(executors.values().size());

        long executorMemory = Utils.parseMemory((String) this.getConfigVal(this.app.getConfig(), "spark.executor.memory", String.class.getName()));
        long driverMemory = Utils.parseMemory(this.isClientMode(app.getConfig())
            ? (String) this.getConfigVal(this.app.getConfig(), "spark.yarn.am.memory", String.class.getName())
            : (String) this.getConfigVal(app.getConfig(), "spark.driver.memory", String.class.getName()));

        int executorCore = (Integer) this.getConfigVal(app.getConfig(), "spark.executor.cores", Integer.class.getName());
        int driverCore = this.isClientMode(app.getConfig())
            ? (Integer) this.getConfigVal(app.getConfig(), "spark.yarn.am.cores", Integer.class.getName())
            : (Integer) this.getConfigVal(app.getConfig(), "spark.driver.cores", Integer.class.getName());

        long executorMemoryOverhead = this.getMemoryOverhead(app.getConfig(), executorMemory, "spark.yarn.executor.memoryOverhead");
        long driverMemoryOverhead = this.isClientMode(app.getConfig())
            ? this.getMemoryOverhead(app.getConfig(), driverMemory, "spark.yarn.am.memoryOverhead")
            : this.getMemoryOverhead(app.getConfig(), driverMemory, "spark.yarn.driver.memoryOverhead");

        app.setExecMemoryBytes(executorMemory);
        app.setDriveMemoryBytes(driverMemory);
        app.setExecutorCores(executorCore);
        app.setDriverCores(driverCore);
        app.setExecutorMemoryOverhead(executorMemoryOverhead);
        app.setDriverMemoryOverhead(driverMemoryOverhead);

        for (SparkExecutor executor : executors.values()) {
            String executorID = executor.getTags().get(SparkJobTagName.SPARK_EXECUTOR_ID.toString());
            if (executorID.equalsIgnoreCase("driver")) {
                executor.setExecMemoryBytes(driverMemory);
                executor.setCores(driverCore);
                executor.setMemoryOverhead(driverMemoryOverhead);
            } else {
                executor.setExecMemoryBytes(executorMemory);
                executor.setCores(executorCore);
                executor.setMemoryOverhead(executorMemoryOverhead);
            }
            if (app.getEndTime() <= 0L) {
                app.setEndTime(this.lastEventTime);
            }
            if (executor.getEndTime() <= 0L) {
                executor.setEndTime(app.getEndTime());
            }
            this.aggregateExecutorToApp(executor);
        }
        this.flushEntities(executors.values(), false);
        //spark code...tricky
        app.setSkippedTasks(app.getCompleteTasks());
        this.flushEntities(app, true);
    }

    private long getMemoryOverhead(JobConfig config, long executorMemory, String fieldName) {
        long result = 0L;
        String fieldValue = config.getConfig().get(fieldName);
        if (fieldValue != null) {
            result = Utils.parseMemory(fieldValue + "m");
            if (result == 0L) {
               result = Utils.parseMemory(fieldValue);
            }
        }

        if (result == 0L) {
            result = Math.max(
                    Utils.parseMemory(conf.getString("spark.defaultVal.spark.yarn.overhead.min")),
                    executorMemory * conf.getInt("spark.defaultVal." + fieldName + ".factor") / 100);
        }
        return result;
    }

    private void aggregateExecutorToApp(SparkExecutor executor) {
        long totalExecutorTime = app.getTotalExecutorTime() + executor.getEndTime() - executor.getStartTime();
        if (totalExecutorTime < 0L) {
            totalExecutorTime = 0L;
        }
        app.setTotalExecutorTime(totalExecutorTime);
    }

    private void aggregateJobToApp(SparkJob job) {
        //aggregate job level metrics
        app.setNumJobs(app.getNumJobs() + 1);
        app.setTotalTasks(app.getTotalTasks() + job.getNumTask());
        app.setCompleteTasks(app.getCompleteTasks() + job.getNumCompletedTasks());
        app.setSkippedTasks(app.getSkippedTasks() + job.getNumSkippedTasks());
        app.setFailedTasks(app.getFailedTasks() + job.getNumFailedTasks());
        app.setTotalStages(app.getTotalStages() + job.getNumStages());
        app.setFailedStages(app.getFailedStages() + job.getNumFailedStages());
        app.setSkippedStages(app.getSkippedStages() + job.getNumSkippedStages());
    }

    private void aggregateStageToApp(SparkStage stage) {
        //aggregate task level metrics
        app.setDiskBytesSpilled(app.getDiskBytesSpilled() + stage.getDiskBytesSpilled());
        app.setMemoryBytesSpilled(app.getMemoryBytesSpilled() + stage.getMemoryBytesSpilled());
        app.setExecutorRunTime(app.getExecutorRunTime() + stage.getExecutorRunTime());
        app.setJvmGcTime(app.getJvmGcTime() + stage.getJvmGcTime());
        app.setExecutorDeserializeTime(app.getExecutorDeserializeTime() + stage.getExecutorDeserializeTime());
        app.setResultSerializationTime(app.getResultSerializationTime() + stage.getResultSerializationTime());
        app.setResultSize(app.getResultSize() + stage.getResultSize());
        app.setInputRecords(app.getInputRecords() + stage.getInputRecords());
        app.setInputBytes(app.getInputBytes() + stage.getInputBytes());
        app.setOutputRecords(app.getOutputRecords() + stage.getOutputRecords());
        app.setOutputBytes(app.getOutputBytes() + stage.getOutputBytes());
        app.setShuffleWriteRecords(app.getShuffleWriteRecords() + stage.getShuffleWriteRecords());
        app.setShuffleWriteBytes(app.getShuffleWriteBytes() + stage.getShuffleWriteBytes());
        app.setShuffleReadRecords(app.getShuffleReadRecords() + stage.getShuffleReadRecords());
        app.setShuffleReadBytes(app.getShuffleReadBytes() + stage.getShuffleReadBytes());
    }

    private void aggregateToStage(SparkTask task) {
        String stageId = task.getTags().get(SparkJobTagName.SPARK_SATGE_ID.toString());
        String stageAttemptId = task.getTags().get(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString());
        String key = this.generateStageKey(stageId, stageAttemptId);
        SparkStage stage = stages.get(key);

        stage.setDiskBytesSpilled(stage.getDiskBytesSpilled() + task.getDiskBytesSpilled());
        stage.setMemoryBytesSpilled(stage.getMemoryBytesSpilled() + task.getMemoryBytesSpilled());
        stage.setExecutorRunTime(stage.getExecutorRunTime() + task.getExecutorRunTime());
        stage.setJvmGcTime(stage.getJvmGcTime() + task.getJvmGcTime());
        stage.setExecutorDeserializeTime(stage.getExecutorDeserializeTime() + task.getExecutorDeserializeTime());
        stage.setResultSerializationTime(stage.getResultSerializationTime() + task.getResultSerializationTime());
        stage.setResultSize(stage.getResultSize() + task.getResultSize());
        stage.setInputRecords(stage.getInputRecords() + task.getInputRecords());
        stage.setInputBytes(stage.getInputBytes() + task.getInputBytes());
        stage.setOutputRecords(stage.getOutputRecords() + task.getOutputRecords());
        stage.setOutputBytes(stage.getOutputBytes() + task.getOutputBytes());
        stage.setShuffleWriteRecords(stage.getShuffleWriteRecords() + task.getShuffleWriteRecords());
        stage.setShuffleWriteBytes(stage.getShuffleWriteBytes() + task.getShuffleWriteBytes());
        stage.setShuffleReadRecords(stage.getShuffleReadRecords() + task.getShuffleReadRecords());
        long taskShuffleReadBytes = task.getShuffleReadLocalBytes() + task.getShuffleReadRemoteBytes();
        stage.setShuffleReadBytes(stage.getShuffleReadBytes() + taskShuffleReadBytes);

        boolean success = !task.isFailed();

        Integer taskIndex = Integer.parseInt(task.getTags().get(SparkJobTagName.SPARK_TASK_INDEX.toString()));
        if (stageTaskStatusMap.get(key).containsKey(taskIndex)) {
            //has previous task attempt, retrieved from task index in one stage
            boolean previousResult = stageTaskStatusMap.get(key).get(taskIndex);
            success = previousResult || success;
            if (previousResult != success) {
                stage.setNumFailedTasks(stage.getNumFailedTasks() - 1);
                stage.setNumCompletedTasks(stage.getNumCompletedTasks() + 1);
                stageTaskStatusMap.get(key).put(taskIndex, success);
            }
        } else {
            if (success) {
                stage.setNumCompletedTasks(stage.getNumCompletedTasks() + 1);
            } else {
                stage.setNumFailedTasks(stage.getNumFailedTasks() + 1);
            }
            stageTaskStatusMap.get(key).put(taskIndex, success);
        }

    }

    private void aggregateToExecutor(SparkTask task) {
        String executorId = task.getExecutorId();
        SparkExecutor executor = executors.get(executorId);

        if (null != executor) {
            executor.setTotalTasks(executor.getTotalTasks() + 1);
            if (task.isFailed()) {
                executor.setFailedTasks(executor.getFailedTasks() + 1);
            } else {
                executor.setCompletedTasks(executor.getCompletedTasks() + 1);
            }
            long taskShuffleReadBytes = task.getShuffleReadLocalBytes() + task.getShuffleReadRemoteBytes();
            executor.setTotalShuffleRead(executor.getTotalShuffleRead() + taskShuffleReadBytes);
            executor.setTotalDuration(executor.getTotalDuration() + task.getExecutorRunTime());
            executor.setTotalInputBytes(executor.getTotalInputBytes() + task.getInputBytes());
            executor.setTotalShuffleWrite(executor.getTotalShuffleWrite() + task.getShuffleWriteBytes());
            executor.setTotalDuration(executor.getTotalDuration() + task.getExecutorRunTime());
        }

    }

    private void aggregateToJob(SparkStage stage) {
        int jobId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString()));
        SparkJob job = jobs.get(jobId);
        job.setNumCompletedTasks(job.getNumCompletedTasks() + stage.getNumCompletedTasks());
        job.setNumFailedTasks(job.getNumFailedTasks() + stage.getNumFailedTasks());
        job.setNumTask(job.getNumTask() + stage.getNumTasks());


        if (stage.getStatus().equalsIgnoreCase(SparkEntityConstant.SparkStageStatus.COMPLETE.toString())) {
            //if multiple attempts succeed, just count one
            if (!hasStagePriorAttemptSuccess(stage)) {
                job.setNumCompletedStages(job.getNumCompletedStages() + 1);
            }
        } else {
            job.setNumFailedStages(job.getNumFailedStages() + 1);
        }
    }

    private boolean hasStagePriorAttemptSuccess(SparkStage stage) {
        int stageAttemptId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString()));
        for (int i = 0; i < stageAttemptId; i++) {
            SparkStage previousStage = stages.get(this.generateStageKey(
                    stage.getTags().get(SparkJobTagName.SPARK_SATGE_ID.toString()), Integer.toString(i)));
            if (previousStage.getStatus().equalsIgnoreCase(SparkEntityConstant.SparkStageStatus.COMPLETE.toString())) {
                return true;
            }
        }
        return false;
    }


    private String generateStageKey(String stageId, String stageAttemptId) {
        return stageId + "-" + stageAttemptId;
    }

    private void initiateStage(int jobId, int stageId, int stageAttemptId, String name, int numTasks) {
        SparkStage stage = new SparkStage();
        stage.setTags(new HashMap<>(this.app.getTags()));
        stage.setTimestamp(app.getTimestamp());
        stage.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), Integer.toString(jobId));
        stage.getTags().put(SparkJobTagName.SPARK_SATGE_ID.toString(), Integer.toString(stageId));
        stage.getTags().put(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString(), Integer.toString(stageAttemptId));
        stage.setName(name);
        stage.setNumActiveTasks(0);
        stage.setNumTasks(numTasks);
        stage.setSchedulingPool(this.app.getConfig().getConfig().get("spark.scheduler.pool") == null ?
                "default" : this.app.getConfig().getConfig().get("spark.scheduler.pool"));

        String stageKey = this.generateStageKey(Integer.toString(stageId), Integer.toString(stageAttemptId));
        stages.put(stageKey, stage);
        this.jobStageMap.get(jobId).add(stageKey);
    }


    private SparkExecutor initiateExecutor(String executorID, long startTime) throws Exception {
        if (!executors.containsKey(executorID)) {
            SparkExecutor executor = new SparkExecutor();
            executor.setTags(new HashMap<>(this.app.getTags()));
            executor.getTags().put(SparkJobTagName.SPARK_EXECUTOR_ID.toString(), executorID);
            executor.setStartTime(startTime);
            executor.setTimestamp(app.getTimestamp());

            this.executors.put(executorID, executor);
        }

        return this.executors.get(executorID);
    }

    private String getNormalizedName(String jobName, String assignedName) {
        if (null != assignedName) {
            return assignedName;
        } else {
            return JobNameNormalization.getInstance().normalize(jobName);
        }
    }

    private void flushEntities(Object entity, boolean forceFlush) {
        this.flushEntities(Collections.singletonList(entity), forceFlush);
    }

    private void flushEntities(Collection entities, boolean forceFlush) {
        this.createEntities.addAll(entities);

        if (forceFlush || this.createEntities.size() >= FLUSH_LIMIT) {
            try {
                this.doFlush(this.createEntities);
                this.createEntities.clear();
            } catch (Exception e) {
                LOG.error("Fail to flush entities", e);
            }

        }
    }

    private EagleServiceBaseClient initiateClient() {
        String host = conf.getString("eagleProps.eagle.service.host");
        int port = conf.getInt("eagleProps.eagle.service.port");
        String userName = conf.getString("eagleProps.eagle.service.username");
        String pwd = conf.getString("eagleProps.eagle.service.password");
        client = new EagleServiceClientImpl(host, port, userName, pwd);
        int timeout = conf.getInt("eagleProps.eagle.service.read.timeout");
        client.getJerseyClient().setReadTimeout(timeout * 1000);

        return client;
    }

    private void doFlush(List entities) throws Exception {
        LOG.info("start flushing entities of total number " + entities.size());
        client.create(entities);
        LOG.info("finish flushing entities of total number " + entities.size());
    }
}