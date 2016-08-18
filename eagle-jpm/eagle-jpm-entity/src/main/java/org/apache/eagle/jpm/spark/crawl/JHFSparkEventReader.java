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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.jpm.spark.entity.JobConfig;
import org.apache.eagle.jpm.spark.entity.*;
import org.apache.eagle.jpm.util.JSONUtil;
import org.apache.eagle.jpm.util.JobNameNormalization;
import org.apache.eagle.jpm.util.SparkEntityConstant;
import org.apache.eagle.jpm.util.SparkJobTagName;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.impl.EagleServiceBaseClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JHFSparkEventReader {
    private static final Logger LOG = LoggerFactory.getLogger(JHFSparkEventReader.class);

    public static final int FLUSH_LIMIT = 500;
    private long firstTaskLaunchTime;

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
        LOG.info("Event type: " + eventType);
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

        List<String> jobConfs = conf.getStringList("basic.jobConf.additional.info");
        String[] props = {"spark.yarn.app.id", "spark.executor.memory", "spark.driver.host", "spark.driver.port",
                "spark.driver.memory", "spark.scheduler.pool", "spark.executor.cores", "spark.yarn.am.memory",
                "spark.yarn.am.cores", "spark.yarn.executor.memoryOverhead", "spark.yarn.driver.memoryOverhead", "spark.yarn.am.memoryOverhead", "spark.master"};
        jobConfs.addAll(Arrays.asList(props));
        for (String prop : jobConfs) {
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
        if (config.getConfig().get("spark.master").equalsIgnoreCase("yarn-client")) {
            return true;
        } else {
            return false;
        }
    }


    private void handleAppStarted(JSONObject event) {
        //need update all entities tag before app start
        List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
        entities.addAll(this.executors.values());
        entities.add(this.app);

        long appStartTime = JSONUtil.getLong(event, "Timestamp");
        for (TaggedLogAPIEntity entity : entities) {
            entity.getTags().put(SparkJobTagName.SPARK_APP_ID.toString(), JSONUtil.getString(event, "App ID"));
            entity.getTags().put(SparkJobTagName.SPARK_APP_NAME.toString(), JSONUtil.getString(event, "App Name"));
            // In yarn-client mode, attemptId is not available in the log, so we set attemptId = 1.
            String attemptId = isClientMode(this.app.getConfig()) ? "1" : JSONUtil.getString(event, "App Attempt ID");
            entity.getTags().put(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString(), attemptId);
            // the second argument of getNormalizeName() is changed to null because the original code contains sensitive text
            // original second argument looks like: this.app.getConfig().getConfig().get("xxx"), "xxx" is the sensitive text
            entity.getTags().put(SparkJobTagName.SPARK_APP_NORM_NAME.toString(), this.getNormalizedName(JSONUtil.getString(event, "App Name"), null));
            entity.getTags().put(SparkJobTagName.SPARK_USER.toString(), JSONUtil.getString(event, "User"));

            entity.setTimestamp(appStartTime);
        }

        this.app.setStartTime(appStartTime);
    }

    private void handleExecutorAdd(JSONObject event) throws Exception {
        String executorID = (String) event.get("Executor ID");
        SparkExecutor executor = this.initiateExecutor(executorID, JSONUtil.getLong(event, "Timestamp"));

        JSONObject executorInfo = JSONUtil.getJSONObject(event, "Executor Info");

    }

    private void handleBlockManagerAdd(JSONObject event) throws Exception {
        long maxMemory = JSONUtil.getLong(event, "Maximum Memory");
        long timestamp = JSONUtil.getLong(event, "Timestamp");
        JSONObject blockInfo = JSONUtil.getJSONObject(event, "Block Manager ID");
        String executorID = JSONUtil.getString(blockInfo, "Executor ID");
        String hostport = String.format("%s:%s", JSONUtil.getString(blockInfo, "Host"), JSONUtil.getLong(blockInfo, "Port"));

        SparkExecutor executor = this.initiateExecutor(executorID, timestamp);
        executor.setMaxMemory(maxMemory);
        executor.setHostPort(hostport);
    }

    private void handleTaskStart(JSONObject event) {
        this.initializeTask(event);
    }

    private void handleTaskEnd(JSONObject event) {
        JSONObject taskInfo = JSONUtil.getJSONObject(event, "Task Info");
        Integer taskId = JSONUtil.getInt(taskInfo, "Task ID");
        SparkTask task = null;
        if (tasks.containsKey(taskId)) {
            task = tasks.get(taskId);
        } else {
            return;
        }

        task.setFailed(JSONUtil.getBoolean(taskInfo, "Failed"));
        JSONObject taskMetrics = JSONUtil.getJSONObject(event, "Task Metrics");
        if (null != taskMetrics) {
            task.setExecutorDeserializeTime(JSONUtil.getLong(taskMetrics, "Executor Deserialize Time"));
            task.setExecutorRunTime(JSONUtil.getLong(taskMetrics, "Executor Run Time"));
            task.setJvmGcTime(JSONUtil.getLong(taskMetrics, "JVM GC Time"));
            task.setResultSize(JSONUtil.getLong(taskMetrics, "Result Size"));
            task.setResultSerializationTime(JSONUtil.getLong(taskMetrics, "Result Serialization Time"));
            task.setMemoryBytesSpilled(JSONUtil.getLong(taskMetrics, "Memory Bytes Spilled"));
            task.setDiskBytesSpilled(JSONUtil.getLong(taskMetrics, "Disk Bytes Spilled"));

            JSONObject inputMetrics = JSONUtil.getJSONObject(taskMetrics, "Input Metrics");
            if (null != inputMetrics) {
                task.setInputBytes(JSONUtil.getLong(inputMetrics, "Bytes Read"));
                task.setInputRecords(JSONUtil.getLong(inputMetrics, "Records Read"));
            }

            JSONObject outputMetrics = JSONUtil.getJSONObject(taskMetrics, "Output Metrics");
            if (null != outputMetrics) {
                task.setOutputBytes(JSONUtil.getLong(outputMetrics, "Bytes Written"));
                task.setOutputRecords(JSONUtil.getLong(outputMetrics, "Records Written"));
            }

            JSONObject shuffleWriteMetrics = JSONUtil.getJSONObject(taskMetrics, "Shuffle Write Metrics");
            if (null != shuffleWriteMetrics) {
                task.setShuffleWriteBytes(JSONUtil.getLong(shuffleWriteMetrics, "Shuffle Bytes Written"));
                task.setShuffleWriteRecords(JSONUtil.getLong(shuffleWriteMetrics, "Shuffle Records Written"));
            }

            JSONObject shuffleReadMetrics = JSONUtil.getJSONObject(taskMetrics, "Shuffle Read Metrics");
            if (null != shuffleReadMetrics) {
                task.setShuffleReadLocalBytes(JSONUtil.getLong(shuffleReadMetrics, "Local Bytes Read"));
                task.setShuffleReadRemoteBytes(JSONUtil.getLong(shuffleReadMetrics, "Remote Bytes Read"));
                task.setShuffleReadRecords(JSONUtil.getLong(shuffleReadMetrics, "Total Records Read"));
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
        task.setTags(new HashMap(this.app.getTags()));
        task.setTimestamp(app.getTimestamp());

        task.getTags().put(SparkJobTagName.SPARK_SATGE_ID.toString(), JSONUtil.getLong(event, "Stage ID").toString());
        task.getTags().put(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString(), JSONUtil.getLong(event, "Stage Attempt ID").toString());

        JSONObject taskInfo = JSONUtil.getJSONObject(event, "Task Info");
        int taskId = JSONUtil.getInt(taskInfo, "Task ID");
        task.setTaskId(taskId);

        task.getTags().put(SparkJobTagName.SPARK_TASK_INDEX.toString(), JSONUtil.getInt(taskInfo, "Index").toString());
        task.getTags().put(SparkJobTagName.SPARK_TASK_ATTEMPT_ID.toString(), JSONUtil.getInt(taskInfo, "Attempt").toString());
        long launchTime = JSONUtil.getLong(taskInfo, "Launch Time");
        if (taskId == 0) {
            this.setFirstTaskLaunchTime(launchTime);
        }
        task.setLaunchTime(launchTime);
        task.setExecutorId(JSONUtil.getString(taskInfo, "Executor ID"));
        task.setHost(JSONUtil.getString(taskInfo, "Host"));
        task.setTaskLocality(JSONUtil.getString(taskInfo, "Locality"));
        task.setSpeculative(JSONUtil.getBoolean(taskInfo, "Speculative"));

        tasks.put(task.getTaskId(), task);
        return task;
    }

    private void setFirstTaskLaunchTime(long launchTime) {
        this.firstTaskLaunchTime = launchTime;
    }

    private long getFirstTaskLaunchTime() {
        return this.firstTaskLaunchTime;
    }


    private void handleJobStart(JSONObject event) {
        SparkJob job = new SparkJob();
        job.setTags(new HashMap(this.app.getTags()));
        job.setTimestamp(app.getTimestamp());

        Integer jobId = JSONUtil.getInt(event, "Job ID");
        job.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), jobId.toString());
        job.setSubmissionTime(JSONUtil.getLong(event, "Submission Time"));

        //for complete application, no active stages/tasks
        job.setNumActiveStages(0);
        job.setNumActiveTasks(0);

        this.jobs.put(jobId, job);
        this.jobStageMap.put(jobId, new HashSet<String>());

        JSONArray stages = JSONUtil.getJSONArray(event, "Stage Infos");
        job.setNumStages(stages.size());
        for (int i = 0; i < stages.size(); i++) {
            JSONObject stageInfo = (JSONObject) stages.get(i);
            Integer stageId = JSONUtil.getInt(stageInfo, "Stage ID");
            Integer stageAttemptId = JSONUtil.getInt(stageInfo, "Stage Attempt ID");
            String stageName = JSONUtil.getString(stageInfo, "Stage Name");
            int numTasks = JSONUtil.getInt(stageInfo, "Number of Tasks");
            this.initiateStage(jobId, stageId, stageAttemptId, stageName, numTasks);
        }
    }

    private void handleStageSubmit(JSONObject event) {
        JSONObject stageInfo = JSONUtil.getJSONObject(event, "Stage Info");
        Integer stageId = JSONUtil.getInt(stageInfo, "Stage ID");
        Integer stageAttemptId = JSONUtil.getInt(stageInfo, "Stage Attempt ID");
        String key = this.generateStageKey(stageId.toString(), stageAttemptId.toString());
        stageTaskStatusMap.put(key, new HashMap<Integer, Boolean>());

        if (!stages.containsKey(this.generateStageKey(stageId.toString(), stageAttemptId.toString()))) {
            //may be further attempt for one stage
            String baseAttempt = this.generateStageKey(stageId.toString(), "0");
            if (stages.containsKey(baseAttempt)) {
                SparkStage stage = stages.get(baseAttempt);
                String jobId = stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString());

                String stageName = JSONUtil.getString(event, "Stage Name");
                int numTasks = JSONUtil.getInt(stageInfo, "Number of Tasks");
                this.initiateStage(Integer.parseInt(jobId), stageId, stageAttemptId, stageName, numTasks);
            }
        }

    }

    private void handleStageComplete(JSONObject event) {
        JSONObject stageInfo = JSONUtil.getJSONObject(event, "Stage Info");
        Integer stageId = JSONUtil.getInt(stageInfo, "Stage ID");
        Integer stageAttemptId = JSONUtil.getInt(stageInfo, "Stage Attempt ID");
        String key = this.generateStageKey(stageId.toString(), stageAttemptId.toString());
        SparkStage stage = stages.get(key);

        // If "Submission Time" is not available, use the "Launch Time" of "Task ID" = 0.
        Long submissionTime = JSONUtil.getLong(stageInfo, "Submission Time");
        if (submissionTime == null) {
            submissionTime = this.getFirstTaskLaunchTime();
        }
        stage.setSubmitTime(submissionTime);
        stage.setCompleteTime(JSONUtil.getLong(stageInfo, "Completion Time"));

        if (stageInfo.containsKey("Failure Reason")) {
            stage.setStatus(SparkEntityConstant.SPARK_STAGE_STATUS.FAILED.toString());
        } else {
            stage.setStatus(SparkEntityConstant.SPARK_STAGE_STATUS.COMPLETE.toString());
        }
    }

    private void handleExecutorRemoved(JSONObject event) {
        String executorID = JSONUtil.getString(event, "Executor ID");
        SparkExecutor executor = executors.get(executorID);
        executor.setEndTime(JSONUtil.getLong(event, "Timestamp"));

    }

    private void handleJobEnd(JSONObject event) {
        Integer jobId = JSONUtil.getInt(event, "Job ID");
        SparkJob job = jobs.get(jobId);
        job.setCompletionTime(JSONUtil.getLong(event, "Completion Time"));
        JSONObject jobResult = JSONUtil.getJSONObject(event, "Job Result");
        String result = JSONUtil.getString(jobResult, "Result");
        if (result.equalsIgnoreCase("JobSucceeded")) {
            job.setStatus(SparkEntityConstant.SPARK_JOB_STATUS.SUCCEEDED.toString());
        } else {
            job.setStatus(SparkEntityConstant.SPARK_JOB_STATUS.FAILED.toString());
        }
    }

    private void handleAppEnd(JSONObject event) {
        long endTime = JSONUtil.getLong(event, "Timestamp");
        app.setEndTime(endTime);
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
            Integer jobId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString()));
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
        long executorMemory = parseExecutorMemory((String) this.getConfigVal(this.app.getConfig(), "spark.executor.memory", String.class.getName()));
        long driverMemory = parseExecutorMemory(this.isClientMode(app.getConfig()) ? (String) this.getConfigVal(this.app.getConfig(), "spark.yarn.am.memory", String.class.getName()) : (String) this.getConfigVal(app.getConfig(), "spark.driver.memory", String.class.getName()));
        int executoreCore = (Integer) this.getConfigVal(app.getConfig(), "spark.executor.cores", Integer.class.getName());
        int driverCore = this.isClientMode(app.getConfig()) ? (Integer) this.getConfigVal(app.getConfig(), "spark.yarn.am.cores", Integer.class.getName()) : (Integer) this.getConfigVal(app.getConfig(), "spark.driver.cores", Integer.class.getName());
        long executorMemoryOverhead = this.getMemoryOverhead(app.getConfig(), executorMemory, "spark.yarn.executor.memoryOverhead");
        long driverMemoryOverhead = this.isClientMode(app.getConfig()) ? this.getMemoryOverhead(app.getConfig(), driverMemory, "spark.yarn.am.memoryOverhead") : this.getMemoryOverhead(app.getConfig(), driverMemory, "spark.yarn.driver.memoryOverhead");

        app.setExecMemoryBytes(executorMemory);
        app.setDriveMemoryBytes(driverMemory);
        app.setExecutorCores(executoreCore);
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
                executor.setCores(executoreCore);
                executor.setMemoryOverhead(executorMemoryOverhead);
            }
            if (executor.getEndTime() == 0)
                executor.setEndTime(app.getEndTime());
            this.aggregateExecutorToApp(executor);
        }
        this.flushEntities(executors.values(), false);
        //spark code...tricky
        app.setSkippedTasks(app.getCompleteTasks());
        this.flushEntities(app, true);
    }

    private long getMemoryOverhead(JobConfig config, long executorMemory, String fieldName) {
        long result = 0l;
        if (config.getConfig().containsKey(fieldName)) {
            result = this.parseExecutorMemory(config.getConfig().get(fieldName) + "m");
            if(result  == 0l){
               result = this.parseExecutorMemory(config.getConfig().get(fieldName));
            }
        }

        if(result == 0l){
            result =  Math.max(this.parseExecutorMemory(conf.getString("spark.defaultVal.spark.yarn.overhead.min")), executorMemory * conf.getInt("spark.defaultVal." + fieldName + ".factor") / 100);
        }
        return result;
    }

    private void aggregateExecutorToApp(SparkExecutor executor) {
        app.setTotalExecutorTime(app.getTotalExecutorTime() + (executor.getEndTime() - executor.getStartTime()));
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
        Integer jobId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_JOB_ID.toString()));
        SparkJob job = jobs.get(jobId);
        job.setNumCompletedTasks(job.getNumCompletedTasks() + stage.getNumCompletedTasks());
        job.setNumFailedTasks(job.getNumFailedTasks() + stage.getNumFailedTasks());
        job.setNumTask(job.getNumTask() + stage.getNumTasks());


        if (stage.getStatus().equalsIgnoreCase(SparkEntityConstant.SPARK_STAGE_STATUS.COMPLETE.toString())) {
            //if multiple attempts succeed, just count one
            if (!hasStagePriorAttemptSuccess(stage)) {
                job.setNumCompletedStages(job.getNumCompletedStages() + 1);
            }

        } else {
            job.setNumFailedStages(job.getNumFailedStages() + 1);
        }
    }

    private boolean hasStagePriorAttemptSuccess(SparkStage stage) {
        Integer stageAttemptId = Integer.parseInt(stage.getTags().get(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString()));
        for (Integer i = 0; i < stageAttemptId; i++) {
            SparkStage previousStage = stages.get(this.generateStageKey(stage.getTags().get(SparkJobTagName.SPARK_SATGE_ID.toString()), i.toString()));
            if (previousStage.getStatus().equalsIgnoreCase(SparkEntityConstant.SPARK_STAGE_STATUS.COMPLETE.toString())) {
                return true;
            }
        }
        return false;
    }


    private String generateStageKey(String stageId, String stageAttemptId) {
        return String.format("%s-%s", stageId, stageAttemptId);
    }

    private void initiateStage(Integer jobId, Integer stageId, Integer stageAttemptId, String name, int numTasks) {
        SparkStage stage = new SparkStage();
        stage.setTags(new HashMap(this.app.getTags()));
        stage.setTimestamp(app.getTimestamp());
        stage.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), jobId.toString());
        stage.getTags().put(SparkJobTagName.SPARK_SATGE_ID.toString(), stageId.toString());
        stage.getTags().put(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString(), stageAttemptId.toString());
        stage.setName(name);
        stage.setNumActiveTasks(0);
        stage.setNumTasks(numTasks);
        stage.setSchedulingPool(this.app.getConfig().getConfig().get("spark.scheduler.pool") == null ? "default" : this.app.getConfig().getConfig().get("spark.scheduler.pool"));

        String stageKey = this.generateStageKey(stageId.toString(), stageAttemptId.toString());
        stages.put(stageKey, stage);
        this.jobStageMap.get(jobId).add(stageKey);
    }


    private SparkExecutor initiateExecutor(String executorID, long startTime) throws Exception {
        if (!executors.containsKey(executorID)) {
            SparkExecutor executor = new SparkExecutor();
            executor.setTags(new HashMap(this.app.getTags()));
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

    private long parseExecutorMemory(String memory) {

        if (memory.endsWith("g") || memory.endsWith("G")) {
            int executorGB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * executorGB;
        } else if (memory.endsWith("m") || memory.endsWith("M")) {
            int executorMB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * executorMB;
        } else if (memory.endsWith("k") || memory.endsWith("K")) {
            int executorKB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * executorKB;
        } else if (memory.endsWith("t") || memory.endsWith("T")) {
            int executorTB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * 1024 * executorTB;
        } else if (memory.endsWith("p") || memory.endsWith("P")) {
            int executorPB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * 1024 * 1024 * executorPB;
        }
        LOG.info("Cannot parse memory info " +  memory);
        return 0l;
    }

    private void flushEntities(Object entity, boolean forceFlush) {
        this.flushEntities(Arrays.asList(entity), forceFlush);
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
        String userName = conf.getString("eagleProps.eagle.service.userName");
        String pwd = conf.getString("eagleProps.eagle.service.pwd");
        client = new EagleServiceClientImpl(host, port, userName, pwd);
        int timeout = conf.getInt("eagleProps.eagle.service.read_timeout");
        client.getJerseyClient().setReadTimeout(timeout * 1000);

        return client;
    }

    private void doFlush(List entities) throws Exception {
        LOG.info("start flushing entities of total number " + entities.size());
//        client.create(entities);
        LOG.info("finish flushing entities of total number " + entities.size());
//        for(Object entity: entities){
//            if(entity instanceof SparkApp){
//                for (Field field : entity.getClass().getDeclaredFields()) {
//                    field.setAccessible(true); // You might want to set modifier to public first.
//                    Object value = field.get(entity);
//                    if (value != null) {
//                        System.out.println(field.getName() + "=" + value);
//                    }
//                }
//            }
//        }
    }


}
