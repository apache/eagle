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

package org.apache.eagle.jpm.spark.running.parser;

import org.apache.eagle.jpm.spark.running.SparkRunningJobAppConfig;
import org.apache.eagle.jpm.spark.running.entities.*;
import org.apache.eagle.jpm.spark.running.recover.SparkRunningJobManager;
import org.apache.eagle.jpm.util.*;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Function;

public class SparkApplicationParser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkApplicationParser.class);

    public enum ParserStatus {
        RUNNING,
        FINISHED,
        APP_FINISHED
    }

    private AppInfo app;
    private static final int MAX_RETRY_TIMES = 2;
    private SparkAppEntityCreationHandler sparkAppEntityCreationHandler;
    //<sparkAppId, SparkAppEntity>
    private Map<String, SparkAppEntity> sparkAppEntityMap;
    private Map<String, JobConfig> sparkJobConfigs;
    private Map<Integer, Pair<Integer, Pair<Long, Long>>> stagesTime;
    private Set<Integer> completeStages;
    private Configuration hdfsConf;
    private SparkRunningJobAppConfig.EndpointConfig endpointConfig;
    private final Object lock = new Object();
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private Map<String, String> commonTags = new HashMap<>();
    private SparkRunningJobManager sparkRunningJobManager;
    private ParserStatus parserStatus;
    private ResourceFetcher rmResourceFetcher;
    private int currentAttempt;
    private boolean first;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public SparkApplicationParser(SparkRunningJobAppConfig.EagleServiceConfig eagleServiceConfig,
                                  SparkRunningJobAppConfig.EndpointConfig endpointConfig,
                                  SparkRunningJobAppConfig.JobExtractorConfig jobExtractorConfig,
                                  AppInfo app, Map<String, SparkAppEntity> sparkApp,
                                  SparkRunningJobManager sparkRunningJobManager, ResourceFetcher rmResourceFetcher) {
        this.sparkAppEntityCreationHandler = new SparkAppEntityCreationHandler(eagleServiceConfig);
        this.endpointConfig = endpointConfig;
        this.app = app;
        this.sparkJobConfigs = new HashMap<>();
        this.stagesTime = new HashMap<>();
        this.completeStages = new HashSet<>();
        this.sparkAppEntityMap = sparkApp;
        if (this.sparkAppEntityMap == null) {
            this.sparkAppEntityMap = new HashMap<>();
        }
        this.rmResourceFetcher = rmResourceFetcher;
        this.currentAttempt = 1;
        this.first = true;
        this.hdfsConf  = new Configuration();
        for (Map.Entry<String, String> entry : endpointConfig.hdfs.entrySet()) {
            this.hdfsConf.set(entry.getKey(), entry.getValue());
            LOG.info("conf key {}, conf value {}", entry.getKey(), entry.getValue());
        }
        this.hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);

        this.commonTags.put(SparkJobTagName.SITE.toString(), jobExtractorConfig.site);
        this.commonTags.put(SparkJobTagName.SPARK_USER.toString(), app.getUser());
        this.commonTags.put(SparkJobTagName.SPARK_QUEUE.toString(), app.getQueue());
        this.parserStatus  = ParserStatus.FINISHED;
        this.sparkRunningJobManager = sparkRunningJobManager;
    }

    public ParserStatus status() {
        return this.parserStatus;
    }

    public void setStatus(ParserStatus status) {
        this.parserStatus = status;
    }

    private void finishSparkApp(String sparkAppId) {
        SparkAppEntity attemptEntity = sparkAppEntityMap.get(sparkAppId);
        attemptEntity.setYarnState(Constants.AppState.FINISHED.toString());
        attemptEntity.setYarnStatus(Constants.AppStatus.FAILED.toString());
        sparkJobConfigs.remove(sparkAppId);
        if (sparkJobConfigs.size() == 0) {
            this.parserStatus = ParserStatus.APP_FINISHED;
        }
        stagesTime.clear();
        LOG.info("spark application {} has been finished", sparkAppId);
    }

    private void fetchSparkRunningInfo() throws Exception {
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            if (fetchSparkApps()) {
                break;
            } else if (i == MAX_RETRY_TIMES - 1) {
                //check whether the app has finished. if we test that we can connect rm, then we consider the app has finished
                //if we get here either because of cannot connect rm or the app has finished
                rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
                sparkAppEntityMap.keySet().forEach(this::finishSparkApp);
                return;
            }
        }

        List<Function<String, Boolean>> functions = new ArrayList<>();
        functions.add(fetchSparkExecutors);
        functions.add(fetchSparkJobs);
        if (!first) {
            functions.add(fetchSparkStagesAndTasks);
        }

        this.first = false;
        for (String sparkAppId : sparkAppEntityMap.keySet()) {
            for (Function<String, Boolean> function : functions) {
                int i = 0;
                for (; i < MAX_RETRY_TIMES; i++) {
                    if (function.apply(sparkAppId)) {
                        break;
                    }
                }
                if (i >= MAX_RETRY_TIMES) {
                    //may caused by rm unreachable
                    rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
                    finishSparkApp(sparkAppId);
                    break;
                }
            }
        }
    }

    @Override
    public void run() {
        synchronized (this.lock) {
            if (this.parserStatus == ParserStatus.APP_FINISHED) {
                return;
            }

            LOG.info("start to process yarn application " + app.getId());
            try {
                fetchSparkRunningInfo();
            } catch (Exception e) {
                LOG.warn("exception found when process application {}, {}", app.getId(), e);
                e.printStackTrace();
            } finally {
                for (String jobId : sparkAppEntityMap.keySet()) {
                    sparkAppEntityCreationHandler.add(sparkAppEntityMap.get(jobId));
                }
                if (sparkAppEntityCreationHandler.flush()) { //force flush
                    //we must flush entities before delete from zk in case of missing finish state of jobs
                    //delete from zk if needed
                    sparkAppEntityMap.keySet()
                        .stream()
                        .filter(
                            jobId -> sparkAppEntityMap.get(jobId).getYarnState().equals(Constants.AppState.FINISHED.toString())
                                || sparkAppEntityMap.get(jobId).getYarnState().equals(Constants.AppState.FAILED.toString()))
                        .forEach(
                            jobId -> this.sparkRunningJobManager.delete(app.getId(), jobId));
                }

                LOG.info("finish process yarn application " + app.getId());
            }

            if (this.parserStatus == ParserStatus.RUNNING) {
                this.parserStatus = ParserStatus.FINISHED;
            }
        }
    }

    private JobConfig parseJobConfig(InputStream is) throws Exception {
        JobConfig jobConfig = new JobConfig();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            boolean stop = false;
            while ((line = reader.readLine()) != null && !stop) {
                try {
                    JSONParser parser = new JSONParser();
                    JSONObject eventObj = (JSONObject) parser.parse(line);

                    if (eventObj != null) {
                        String eventType = (String) eventObj.get("Event");
                        LOG.info("Event type: " + eventType);
                        if (eventType.equalsIgnoreCase(SparkEventType.SparkListenerEnvironmentUpdate.toString())) {
                            stop = true;
                            JSONObject sparkProps = (JSONObject) eventObj.get("Spark Properties");
                            for (Object key : sparkProps.keySet()) {
                                jobConfig.put((String) key, (String) sparkProps.get(key));
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error(String.format("Fail to parse %s.", line), e);
                }
            }
            
            return jobConfig;
        }
    }

    private JobConfig getJobConfig(String sparkAppId, int attemptId) {
        //TODO: getResourceManagerVersion() and compare version to make attempt id.

        LOG.info("Get job config for sparkAppId {}, attempt {}, appId {}", sparkAppId, attemptId, app.getId());
        JobConfig jobConfig = null;

        try (FileSystem hdfs = HDFSUtil.getFileSystem(this.hdfsConf)) {
            //             // For Yarn version >= 2.7,
            //             // log name: "application_1468625664674_0003_appattempt_1468625664674_0003_000001"
            //             String attemptIdFormatted = String.format("%06d", attemptId);
            //             // remove "application_" to get the number part of appID.
            //             String sparkAppIdNum = sparkAppId.substring(12);
            //             String attemptIdString = "appattempt_" + sparkAppIdNum + "_" + attemptIdFormatted;

            // For Yarn version 2.4.x
            // log name: application_1464382345557_269065_1
            String attemptIdString = Integer.toString(attemptId);

            //test appId_attemptId.inprogress/appId_attemptId/appId.inprogress/appId
            String eventLogDir = this.endpointConfig.eventLog;
            Path attemptFile = new Path(eventLogDir + "/" + sparkAppId + "_" + attemptIdString + ".inprogress");
            if (!hdfs.exists(attemptFile)) {
                attemptFile = new Path(eventLogDir + "/" + sparkAppId + "_" + attemptIdString);
                if (!hdfs.exists(attemptFile)) {
                    attemptFile = new Path(eventLogDir + "/" + sparkAppId + ".inprogress");
                    if (!hdfs.exists(attemptFile)) {
                        attemptFile = new Path(eventLogDir + "/" + sparkAppId);
                    }
                }
            }

            LOG.info("Attempt File path: " + attemptFile.toString());
            jobConfig = parseJobConfig(hdfs.open(attemptFile));
        } catch (Exception e) {
            LOG.error("Fail to process application {}", sparkAppId, e);
        }

        return jobConfig;
    }

    private boolean isClientMode(JobConfig jobConfig) {
        return jobConfig.containsKey(Constants.SPARK_MASTER_KEY)
            && jobConfig.get(Constants.SPARK_MASTER_KEY).equalsIgnoreCase("yarn-client");
    }

    private boolean fetchSparkApps() {
        String appURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        SparkApplication[] sparkApplications = null;
        try {
            is = InputStreamUtils.getInputStream(appURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch spark application from {}", appURL);
            sparkApplications = OBJ_MAPPER.readValue(is, SparkApplication[].class);
        } catch (java.net.ConnectException e) {
            LOG.warn("fetch spark application from {} failed, {}", appURL, e);
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            LOG.warn("fetch spark application from {} failed, {}", appURL, e);
            e.printStackTrace();
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        for (SparkApplication sparkApplication : sparkApplications) {
            String id = sparkApplication.getId();
            if (id.contains(" ") || !id.startsWith("app")) {
                //spark version < 1.6.0 and id contains white space, need research again later
                LOG.warn("skip spark application {}", id);
                continue;
            }

            currentAttempt = sparkApplication.getAttempts().size();
            int lastSavedAttempt = 1;
            if (sparkAppEntityMap.containsKey(id)) {
                lastSavedAttempt = Integer.parseInt(sparkAppEntityMap.get(id).getTags().get(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString()));
            }
            for (int j = lastSavedAttempt; j <= currentAttempt; j++) {
                commonTags.put(SparkJobTagName.SPARK_APP_NAME.toString(), sparkApplication.getName());
                commonTags.put(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString(), "" + j);
                commonTags.put(SparkJobTagName.SPARK_APP_ID.toString(), id);
                SparkAppEntity attemptEntity = new SparkAppEntity();
                attemptEntity.setTags(new HashMap<>(commonTags));
                attemptEntity.setAppInfo(app);

                attemptEntity.setStartTime(Utils.dateTimeToLong(sparkApplication.getAttempts().get(j - 1).getStartTime()));
                attemptEntity.setTimestamp(attemptEntity.getStartTime());

                if (sparkJobConfigs.containsKey(id) && j == currentAttempt) {
                    attemptEntity.setConfig(sparkJobConfigs.get(id));
                }

                if (attemptEntity.getConfig() == null) {
                    attemptEntity.setConfig(getJobConfig(id, j));
                    if (j == currentAttempt) {
                        sparkJobConfigs.put(id, attemptEntity.getConfig());
                    }
                }

                try {
                    JobConfig jobConfig = attemptEntity.getConfig();
                    attemptEntity.setExecMemoryBytes(Utils.parseMemory(jobConfig.get(Constants.SPARK_EXECUTOR_MEMORY_KEY)));

                    attemptEntity.setDriveMemoryBytes(isClientMode(jobConfig)
                        ? 0
                        : Utils.parseMemory(jobConfig.get(Constants.SPARK_DRIVER_MEMORY_KEY)));
                    attemptEntity.setExecutorCores(Integer.parseInt(jobConfig.get(Constants.SPARK_EXECUTOR_CORES_KEY)));
                    // spark.driver.cores may not be set.
                    String driverCoresStr = jobConfig.get(Constants.SPARK_DRIVER_CORES_KEY);
                    int driverCores = 0;
                    if (driverCoresStr != null && !isClientMode(jobConfig)) {
                        driverCores = Integer.parseInt(driverCoresStr);
                    }
                    attemptEntity.setDriverCores(driverCores);
                } catch (Exception e) {
                    LOG.warn("add config failed, {}", e);
                    e.printStackTrace();
                }

                if (j == currentAttempt) {
                    //current attempt
                    attemptEntity.setYarnState(app.getState());
                    attemptEntity.setYarnStatus(app.getFinalStatus());
                    sparkAppEntityMap.put(id, attemptEntity);
                    this.sparkRunningJobManager.update(app.getId(), id, attemptEntity);
                } else {
                    attemptEntity.setYarnState(Constants.AppState.FINISHED.toString());
                    attemptEntity.setYarnStatus(Constants.AppStatus.FAILED.toString());
                }
                sparkAppEntityCreationHandler.add(attemptEntity);
            }
        }

        sparkAppEntityCreationHandler.flush();
        return true;
    }

    private Function<String, Boolean> fetchSparkExecutors = sparkAppId -> {
        //only get current attempt
        SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
        String executorURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_EXECUTORS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        SparkExecutor[] sparkExecutors = null;
        try {
            is = InputStreamUtils.getInputStream(executorURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch spark executor from {}", executorURL);
            sparkExecutors = OBJ_MAPPER.readValue(is, SparkExecutor[].class);
        } catch (java.net.ConnectException e) {
            LOG.warn("fetch spark application from {} failed, {}", executorURL, e);
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            LOG.warn("fetch spark executor from {} failed, {}", executorURL, e);
            e.printStackTrace();
            return false;
        } finally {
            Utils.closeInputStream(is);
        }
        sparkAppEntity.setExecutors(sparkExecutors.length);

        for (SparkExecutor executor : sparkExecutors) {
            SparkExecutorEntity entity = new SparkExecutorEntity();
            entity.setTags(new HashMap<>(sparkAppEntity.getTags()));
            entity.getTags().put(SparkJobTagName.SPARK_EXECUTOR_ID.toString(), executor.getId());
            entity.setHostPort(executor.getHostPort());
            entity.setRddBlocks(executor.getRddBlocks());
            entity.setMemoryUsed(executor.getMemoryUsed());
            entity.setDiskUsed(executor.getDiskUsed());
            entity.setActiveTasks(executor.getActiveTasks());
            entity.setFailedTasks(executor.getFailedTasks());
            entity.setCompletedTasks(executor.getCompletedTasks());
            entity.setTotalTasks(executor.getTotalTasks());
            entity.setTotalDuration(executor.getTotalDuration());
            entity.setTotalInputBytes(executor.getTotalInputBytes());
            entity.setTotalShuffleRead(executor.getTotalShuffleRead());
            entity.setTotalShuffleWrite(executor.getTotalShuffleWrite());
            entity.setMaxMemory(executor.getMaxMemory());

            entity.setTimestamp(sparkAppEntity.getTimestamp());
            entity.setStartTime(sparkAppEntity.getStartTime());
            if (executor.getId().equalsIgnoreCase("driver")) {
                entity.setExecMemoryBytes(sparkAppEntity.getDriveMemoryBytes());
                entity.setCores(sparkAppEntity.getDriverCores());
                entity.setMemoryOverhead(sparkAppEntity.getDriverMemoryOverhead());
            } else {
                entity.setExecMemoryBytes(sparkAppEntity.getExecMemoryBytes());
                entity.setCores(sparkAppEntity.getExecutorCores());
                entity.setMemoryOverhead(sparkAppEntity.getExecutorMemoryOverhead());
            }
            sparkAppEntityCreationHandler.add(entity);
        }
        return true;
    };

    private Function<String, Boolean> fetchSparkJobs = sparkAppId -> {
        //only get current attempt
        SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
        String jobURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_JOBS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        SparkJob[] sparkJobs = null;
        try {
            is = InputStreamUtils.getInputStream(jobURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch spark job from {}", jobURL);
            sparkJobs = OBJ_MAPPER.readValue(is, SparkJob[].class);
        } catch (java.net.ConnectException e) {
            LOG.warn("fetch spark application from {} failed, {}", jobURL, e);
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            LOG.warn("fetch spark job from {} failed, {}", jobURL, e);
            e.printStackTrace();
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        sparkAppEntity.setNumJobs(sparkJobs.length);
        for (SparkJob sparkJob : sparkJobs) {
            SparkJobEntity entity = new SparkJobEntity();
            entity.setTags(new HashMap<>(commonTags));
            entity.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), sparkJob.getJobId() + "");
            entity.setSubmissionTime(Utils.dateTimeToLong(sparkJob.getSubmissionTime()));
            if (sparkJob.getCompletionTime() != null) {
                entity.setCompletionTime(Utils.dateTimeToLong(sparkJob.getCompletionTime()));
            }
            entity.setNumStages(sparkJob.getStageIds().size());
            entity.setStatus(sparkJob.getStatus());
            entity.setNumTask(sparkJob.getNumTasks());
            entity.setNumActiveTasks(sparkJob.getNumActiveTasks());
            entity.setNumCompletedTasks(sparkJob.getNumCompletedTasks());
            entity.setNumSkippedTasks(sparkJob.getNumSkippedTasks());
            entity.setNumFailedTasks(sparkJob.getNumFailedTasks());
            entity.setNumActiveStages(sparkJob.getNumActiveStages());
            entity.setNumCompletedStages(sparkJob.getNumCompletedStages());
            entity.setNumSkippedStages(sparkJob.getNumSkippedStages());
            entity.setNumFailedStages(sparkJob.getNumFailedStages());
            entity.setStages(sparkJob.getStageIds());
            entity.setTimestamp(sparkAppEntity.getTimestamp());

            sparkAppEntity.setTotalStages(sparkAppEntity.getTotalStages() + entity.getNumStages());
            sparkAppEntity.setTotalTasks(sparkAppEntity.getTotalTasks() + entity.getNumTask());
            sparkAppEntity.setActiveTasks(sparkAppEntity.getActiveTasks() + entity.getNumActiveTasks());
            sparkAppEntity.setCompleteTasks(sparkAppEntity.getCompleteTasks() + entity.getNumCompletedTasks());
            sparkAppEntity.setSkippedTasks(sparkAppEntity.getSkippedTasks() + entity.getNumSkippedTasks());
            sparkAppEntity.setFailedTasks(sparkAppEntity.getFailedStages() + entity.getNumFailedTasks());
            sparkAppEntity.setActiveStages(sparkAppEntity.getActiveStages() + entity.getNumActiveStages());
            sparkAppEntity.setCompleteStages(sparkAppEntity.getCompleteStages() + entity.getNumCompletedStages());
            sparkAppEntity.setSkippedStages(sparkAppEntity.getSkippedStages() + entity.getNumSkippedStages());
            sparkAppEntity.setFailedStages(sparkAppEntity.getFailedStages() + entity.getNumFailedStages());

            for (Integer stageId : sparkJob.getStageIds()) {
                stagesTime.put(stageId, Pair.of(sparkJob.getJobId(), Pair.of(entity.getSubmissionTime(), entity.getCompletionTime())));
            }
            sparkAppEntityCreationHandler.add(entity);
        }
        return true;
    };

    private Function<String, Boolean> fetchSparkStagesAndTasks = sparkAppId -> {
        SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
        String stageURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_STAGES_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        SparkStage[] sparkStages;
        try {
            is = InputStreamUtils.getInputStream(stageURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch spark stage from {}", stageURL);
            sparkStages = OBJ_MAPPER.readValue(is, SparkStage[].class);
        } catch (java.net.ConnectException e) {
            LOG.warn("fetch spark application from {} failed, {}", stageURL, e);
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            LOG.warn("fetch spark stage from {} failed, {}", stageURL, e);
            e.printStackTrace();
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        for (SparkStage sparkStage : sparkStages) {
            //TODO
            //we need a thread pool to handle this if there are many stages
            SparkStage stage;
            try {
                stageURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_STAGES_URL + "/" + sparkStage.getStageId() + "?" + Constants.ANONYMOUS_PARAMETER;
                is = InputStreamUtils.getInputStream(stageURL, null, Constants.CompressionType.NONE);
                LOG.info("fetch spark stage from {}", stageURL);
                stage = OBJ_MAPPER.readValue(is, SparkStage[].class)[0];
            } catch (Exception e) {
                LOG.warn("fetch spark stage from {} failed, {}", stageURL, e);
                e.printStackTrace();
                return false;
            } finally {
                Utils.closeInputStream(is);
            }

            if (this.completeStages.contains(stage.getStageId())) {
                return true;
            }
            SparkStageEntity stageEntity = new SparkStageEntity();
            stageEntity.setTags(new HashMap<>(commonTags));
            stageEntity.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), stagesTime.get(stage.getStageId()).getLeft() + "");
            stageEntity.getTags().put(SparkJobTagName.SPARK_STAGE_ID.toString(), stage.getStageId() + "");
            stageEntity.getTags().put(SparkJobTagName.SPARK_STAGE_ATTEMPT_ID.toString(), stage.getAttemptId() + "");
            stageEntity.setStatus(stage.getStatus());
            stageEntity.setNumActiveTasks(stage.getNumActiveTasks());
            stageEntity.setNumCompletedTasks(stage.getNumCompleteTasks());
            stageEntity.setNumFailedTasks(stage.getNumFailedTasks());
            stageEntity.setExecutorRunTime(stage.getExecutorRunTime());
            stageEntity.setInputBytes(stage.getInputBytes());
            stageEntity.setInputRecords(stage.getInputRecords());
            stageEntity.setOutputBytes(stage.getOutputBytes());
            stageEntity.setOutputRecords(stage.getOutputRecords());
            stageEntity.setShuffleReadBytes(stage.getShuffleReadBytes());
            stageEntity.setShuffleReadRecords(stage.getShuffleReadRecords());
            stageEntity.setShuffleWriteBytes(stage.getShuffleWriteBytes());
            stageEntity.setShuffleWriteRecords(stage.getShuffleWriteRecords());
            stageEntity.setMemoryBytesSpilled(stage.getMemoryBytesSpilled());
            stageEntity.setDiskBytesSpilled(stage.getDiskBytesSpilled());
            stageEntity.setName(stage.getName());
            stageEntity.setSchedulingPool(stage.getSchedulingPool());
            stageEntity.setSubmitTime(stagesTime.get(stage.getStageId()).getRight().getLeft());
            stageEntity.setTimestamp(stageEntity.getSubmitTime());
            stageEntity.setCompleteTime(stagesTime.get(stage.getStageId()).getRight().getRight());
            stageEntity.setNumTasks(stage.getTasks() == null ? 0 : stage.getTasks().size());
            fetchTasksFromStage(stageEntity, stage);
            sparkAppEntityCreationHandler.add(stageEntity);
            if (stage.getStatus().equals(Constants.StageState.COMPLETE.toString())) {
                this.completeStages.add(stage.getStageId());
                LOG.info("stage {} of spark {} has finished", stage.getStageId(), sparkAppId);
            }

            sparkAppEntity.setInputBytes(sparkAppEntity.getInputBytes() + stageEntity.getInputBytes());
            sparkAppEntity.setInputRecords(sparkAppEntity.getInputBytes() + stageEntity.getInputRecords());
            sparkAppEntity.setOutputBytes(sparkAppEntity.getOutputBytes() + stageEntity.getOutputBytes());
            sparkAppEntity.setOutputRecords(sparkAppEntity.getOutputBytes() + stageEntity.getOutputRecords());
            sparkAppEntity.setShuffleReadBytes(sparkAppEntity.getShuffleReadBytes() + stageEntity.getShuffleReadBytes());
            sparkAppEntity.setShuffleReadRecords(sparkAppEntity.getShuffleReadRecords() + stageEntity.getShuffleReadRecords());
            sparkAppEntity.setShuffleWriteBytes(sparkAppEntity.getShuffleWriteBytes() + stageEntity.getShuffleWriteBytes());
            sparkAppEntity.setShuffleWriteRecords(sparkAppEntity.getShuffleWriteRecords() + stageEntity.getShuffleWriteRecords());
            sparkAppEntity.setExecutorRunTime(sparkAppEntity.getExecutorRunTime() + stageEntity.getExecutorRunTime());
            sparkAppEntity.setExecutorDeserializeTime(sparkAppEntity.getExecutorDeserializeTime() + stageEntity.getExecutorDeserializeTime());
            sparkAppEntity.setResultSize(sparkAppEntity.getResultSize() + stageEntity.getResultSize());
            sparkAppEntity.setJvmGcTime(sparkAppEntity.getJvmGcTime() + stageEntity.getJvmGcTime());
            sparkAppEntity.setResultSerializationTime(sparkAppEntity.getResultSerializationTime() + stageEntity.getResultSerializationTime());
            sparkAppEntity.setMemoryBytesSpilled(sparkAppEntity.getMemoryBytesSpilled() + stageEntity.getMemoryBytesSpilled());
            sparkAppEntity.setDiskBytesSpilled(sparkAppEntity.getDiskBytesSpilled() + stageEntity.getDiskBytesSpilled());
            sparkAppEntity.setCompleteTasks(sparkAppEntity.getCompleteTasks() + stageEntity.getNumCompletedTasks());
        }
        return true;
    };

    private void fetchTasksFromStage(SparkStageEntity stageEntity, SparkStage stage) {
        Map<String, SparkTask> tasks = stage.getTasks();
        for (String key : tasks.keySet()) {
            SparkTask task = tasks.get(key);
            SparkTaskEntity taskEntity = new SparkTaskEntity();
            taskEntity.setTags(new HashMap<>(stageEntity.getTags()));
            taskEntity.getTags().put(SparkJobTagName.SPARK_TASK_ATTEMPT_ID.toString(), task.getAttempt() + "");
            taskEntity.getTags().put(SparkJobTagName.SPARK_TASK_INDEX.toString(), task.getIndex() + "");
            taskEntity.setTaskId(task.getTaskId());
            taskEntity.setLaunchTime(Utils.dateTimeToLong(task.getLaunchTime()));
            taskEntity.setHost(task.getHost());
            taskEntity.setTaskLocality(task.getTaskLocality());
            taskEntity.setSpeculative(task.isSpeculative());
            taskEntity.setTimestamp(stageEntity.getTimestamp());

            SparkTaskMetrics taskMetrics = task.getTaskMetrics();
            taskEntity.setExecutorDeserializeTime(taskMetrics == null ? 0 : taskMetrics.getExecutorDeserializeTime());
            taskEntity.setExecutorRunTime(taskMetrics == null ? 0 : taskMetrics.getExecutorRunTime());
            taskEntity.setResultSize(taskMetrics == null ? 0 : taskMetrics.getResultSize());
            taskEntity.setJvmGcTime(taskMetrics == null ? 0 : taskMetrics.getJvmGcTime());
            taskEntity.setResultSerializationTime(taskMetrics == null ? 0 : taskMetrics.getResultSerializationTime());
            taskEntity.setMemoryBytesSpilled(taskMetrics == null ? 0 : taskMetrics.getMemoryBytesSpilled());
            taskEntity.setDiskBytesSpilled(taskMetrics == null ? 0 : taskMetrics.getDiskBytesSpilled());

            SparkTaskInputMetrics inputMetrics = null;
            if (taskMetrics != null && taskMetrics.getInputMetrics() != null) {
                inputMetrics = taskMetrics.getInputMetrics();
            }
            taskEntity.setInputBytes(inputMetrics == null ? 0 : inputMetrics.getBytesRead());
            taskEntity.setInputRecords(inputMetrics == null ? 0 : inputMetrics.getRecordsRead());

            //need to figure outputMetrics

            SparkTaskShuffleReadMetrics shuffleReadMetrics = null;
            if (taskMetrics != null && taskMetrics.getShuffleReadMetrics() != null) {
                shuffleReadMetrics = taskMetrics.getShuffleReadMetrics();
            }
            taskEntity.setShuffleReadRemoteBytes(shuffleReadMetrics == null ? 0 : shuffleReadMetrics.getRemoteBytesRead());
            taskEntity.setShuffleReadRecords(shuffleReadMetrics == null ? 0 : shuffleReadMetrics.getRecordsRead());

            SparkTaskShuffleWriteMetrics shuffleWriteMetrics = null;
            if (taskMetrics != null && taskMetrics.getShuffleWriteMetrics() != null) {
                shuffleWriteMetrics = taskMetrics.getShuffleWriteMetrics();
            }
            taskEntity.setShuffleWriteBytes(shuffleWriteMetrics == null ? 0 : shuffleWriteMetrics.getBytesWritten());
            taskEntity.setShuffleWriteRecords(shuffleWriteMetrics == null ? 0 : shuffleWriteMetrics.getRecordsWritten());

            stageEntity.setExecutorDeserializeTime(stageEntity.getExecutorDeserializeTime() + taskEntity.getExecutorDeserializeTime());
            stageEntity.setResultSize(stageEntity.getResultSize() + taskEntity.getResultSize());
            stageEntity.setJvmGcTime(stageEntity.getJvmGcTime() + taskEntity.getJvmGcTime());
            stageEntity.setResultSerializationTime(stageEntity.getResultSerializationTime() + taskEntity.getResultSerializationTime());

            this.sparkAppEntityCreationHandler.add(taskEntity);
        }
    }
}
