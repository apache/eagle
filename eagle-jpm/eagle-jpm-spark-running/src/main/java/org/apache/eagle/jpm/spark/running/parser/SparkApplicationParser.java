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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.jpm.spark.running.entities.*;
import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.jpm.spark.running.entities.JobConfig;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.SparkJobTagName;
import org.apache.eagle.jpm.util.resourceFetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourceFetch.model.*;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkExecutor;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkJob;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkStage;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.function.Function;

public class SparkApplicationParser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkApplicationParser.class);

    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private AppInfo app;
    private static final int MAX_ENTITIES_SIZE = 500;
    private static final int MAX_RETRY_TIMES = 3;
    private IEagleServiceClient client;

    //<sparkAppId, SparkAppEntity>
    private Map<String, SparkAppEntity> sparkAppEntityMap;
    private Map<String, JobConfig> sparkJobConfigs;
    private Map<Integer, Pair<Integer, Pair<Long, Long>>> stagesTime = new HashMap<>();
    private Configuration hdfsConf;
    private SparkRunningConfigManager.EndpointConfig endpointConfig;
    private SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig;
    private final Object lock = new Object();
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private Map<String, String> commonTags = new HashMap<>();

    private int currentAttempt = 1;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public SparkApplicationParser(SparkRunningConfigManager.EagleServiceConfig eagleServiceConfig,
                                  SparkRunningConfigManager.EndpointConfig endpointConfig,
                                  SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig,
                                  AppInfo app, Map<String, SparkAppEntity> sparkApp) {
        this.app = app;
        this.sparkJobConfigs = new HashMap<>();
        this.sparkAppEntityMap = sparkApp;
        if (this.sparkAppEntityMap == null) {
            this.sparkAppEntityMap = new HashMap<>();
        }
        this.client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.hdfsConf  = new Configuration();
        this.hdfsConf.set("fs.defaultFS", endpointConfig.nnEndpoint);
        this.hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        this.hdfsConf.set("hdfs.kerberos.principal", endpointConfig.principal);
        this.hdfsConf.set("hdfs.keytab.file", endpointConfig.keyTab);

        this.commonTags.put(SparkJobTagName.SITE.toString(), jobExtractorConfig.site);
        this.commonTags.put(SparkJobTagName.SPARK_USER.toString(), app.getUser());
        this.commonTags.put(SparkJobTagName.SPARK_QUEUE.toString(), app.getQueue());
    }

    private void flush(TaggedLogAPIEntity entity) {
        if (entity != null) entities.add(entity);
        try {
            if (entities.size() >= MAX_ENTITIES_SIZE) {
                LOG.info("start to flush spark entities for application {}, size {}", app.getId(), entities.size());
                //client.create(entities);
                LOG.info("finish flushing spark entities for application {}, size {}", app.getId(), entities.size());
                entities.clear();
            }
        } catch (Exception e) {
            LOG.warn("exception found when flush entities {}", e);
            e.printStackTrace();
        }
    }

    private void closeInputStream(InputStream is) {
        if (is != null) {
            try {
                is.close();
            } catch (Exception e) {
            }
        }
    }

    private long dateTimeToLong(String date) {
        //TODO
        return 0l;
    }

    private void finishSparkApp(String sparkAppId) {
        SparkAppEntity attemptEntity = sparkAppEntityMap.get(sparkAppId);
        attemptEntity.setYarnState(Constants.AppState.FINISHED.toString());
        attemptEntity.setYarnStatus(Constants.AppStatus.FAILED.toString());
        flush(attemptEntity);

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
                new RMResourceFetcher(endpointConfig.rmUrls).getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
                for (String sparkAppId : sparkAppEntityMap.keySet()) {
                    finishSparkApp(sparkAppId);
                }
            }
        }

        List<Function<String, Boolean>> functions = new ArrayList<>();
        functions.add(fetchSparkExecutors);
        functions.add(fetchSparkJobs);
        functions.add(fetchSparkStagesAndTasks);
        for (String sparkAppId : sparkAppEntityMap.keySet()) {
            for (Function<String, Boolean> function : functions) {
                int i = 0;
                for (; i < MAX_RETRY_TIMES; i++) {
                    if (function.apply(sparkAppId)) {
                        break;
                    }
                }
                if (i >= MAX_RETRY_TIMES) {
                    finishSparkApp(sparkAppId);
                    break;
                }
            }
        }
    }

    @Override
    public void run() {
        synchronized (this.lock) {
            LOG.info("start to process yarn application " + app.getId());
            try {
                fetchSparkRunningInfo();
                flush(null);
            } catch (Exception e) {
                LOG.warn("exception found when process application {}, {}", app.getId(), e);
                e.printStackTrace();
            } finally {
                LOG.info("finish process yarn application " + app.getId());
            }
        }
    }

    private JobConfig getJobConfig(String sparkAppId, int attemptId) {
        //TODO
        LOG.info("get job config for {}, attempt {}, appId, {}", sparkAppId, attemptId, app.getId());
        return new JobConfig();
    }

    private boolean fetchSparkApps() {
        String appURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        SparkApplication[] sparkApplications = null;
        try {
            is = InputStreamUtils.getInputStream(appURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch spark application from {}", appURL);
            sparkApplications = OBJ_MAPPER.readValue(is, SparkApplication[].class);
        } catch (Exception e) {
            LOG.warn("fetch spark application from {} failed, {}", appURL, e);
            e.printStackTrace();
            return false;
        } finally {
            closeInputStream(is);
        }
        for (SparkApplication sparkApplication : sparkApplications) {
            String id = sparkApplication.getId();
            if (id.contains(" ")) {
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
                SparkAppEntity attemptEntity = new SparkAppEntity();
                commonTags.put(SparkJobTagName.SPARK_APP_NAME.toString(), sparkApplication.getName());
                commonTags.put(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString(), "" + j);
                commonTags.put(SparkJobTagName.SPARK_APP_ID.toString(), id);
                attemptEntity.setTags(new HashMap<>(commonTags));
                attemptEntity.setAppInfo(app);

                attemptEntity.setStartTime(dateTimeToLong(sparkApplication.getAttempts().get(j - 1).getStartTime()));
                attemptEntity.setTimestamp(attemptEntity.getStartTime());

                if (sparkJobConfigs.containsKey(id) && j == currentAttempt) {
                    attemptEntity.setConfig(sparkJobConfigs.get(id));
                }

                if (attemptEntity.getConfig() == null) {
                    //read config from hdfs
                    attemptEntity.setConfig(getJobConfig(id, j));
                    if (j == currentAttempt) {
                        sparkJobConfigs.put(id, attemptEntity.getConfig());
                    }
                }

                if (j == currentAttempt) {
                    //current attempt
                    attemptEntity.setYarnState(app.getState());
                    attemptEntity.setYarnStatus(app.getFinalStatus());
                    sparkAppEntityMap.put(id, attemptEntity);
                    //TODO, save to zookeeper(override)
                } else {
                    attemptEntity.setYarnState(Constants.AppState.FAILED.toString());
                    attemptEntity.setYarnStatus(Constants.AppStatus.FAILED.toString());
                    flush(attemptEntity);
                }
            }
        }
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
        } catch (Exception e) {
            LOG.warn("fetch spark executor from {} failed, {}", executorURL, e);
            e.printStackTrace();
            return false;
        } finally {
            closeInputStream(is);
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
            entity.setExecMemoryBytes(sparkAppEntity.getExecMemoryBytes());
            entity.setCores(sparkAppEntity.getExecutorCores());
            entity.setMemoryOverhead(sparkAppEntity.getExecutorMemoryOverhead());
            flush(entity);
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
        } catch (Exception e) {
            LOG.warn("fetch spark job from {} failed, {}", jobURL, e);
            e.printStackTrace();
            return false;
        } finally {
            closeInputStream(is);
        }

        sparkAppEntity.setNumJobs(sparkJobs.length);
        for (SparkJob sparkJob : sparkJobs) {
            SparkJobEntity entity = new SparkJobEntity();
            entity.setTags(new HashMap<>(commonTags));
            entity.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), sparkJob.getJobId() + "");
            entity.setSubmissionTime(dateTimeToLong(sparkJob.getSubmissionTime()));
            if (sparkJob.getCompletionTime() != null) {
                entity.setCompletionTime(dateTimeToLong(sparkJob.getCompletionTime()));
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
            flush(entity);
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
        } catch (Exception e) {
            LOG.warn("fetch spark job from {} failed, {}", stageURL, e);
            e.printStackTrace();
            return false;
        } finally {
            closeInputStream(is);
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
                LOG.warn("fetch spark job from {} failed, {}", stageURL, e);
                e.printStackTrace();
                return false;
            } finally {
                closeInputStream(is);
            }
            SparkStageEntity stageEntity = new SparkStageEntity();
            stageEntity.setTags(new HashMap<>(commonTags));
            stageEntity.getTags().put(SparkJobTagName.SPARK_JOB_ID.toString(), stagesTime.get(stage.getStageId()).getLeft() + "");
            stageEntity.getTags().put(SparkJobTagName.SPARK_SATGE_ID.toString(), stage.getStageId() + "");
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
            stageEntity.setCompleteTime(stagesTime.get(stage.getStageId()).getRight().getRight());
            stageEntity.setNumTasks(stage.getTasks() == null ? 0 : stage.getTasks().size());
            fetchTasksFromStage(sparkAppEntity, stageEntity, stage);
            flush(stageEntity);

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

    private void fetchTasksFromStage(SparkAppEntity sparkAppEntity, SparkStageEntity stageEntity, SparkStage stage) {
        Map<String, SparkTask> tasks = stage.getTasks();
        for (String key : tasks.keySet()) {
            SparkTask task = tasks.get(key);
            SparkTaskEntity taskEntity = new SparkTaskEntity();
            taskEntity.setTags(new HashMap<>(stageEntity.getTags()));
            taskEntity.getTags().put(SparkJobTagName.SPARK_TASK_ATTEMPT_ID.toString(), task.getAttempt() + "");
            taskEntity.getTags().put(SparkJobTagName.SPARK_TASK_INDEX.toString(), task.getIndex() + "");
            taskEntity.setTaskId(task.getTaskId());
            taskEntity.setLaunchTime(dateTimeToLong(task.getLaunchTime()));
            taskEntity.setHost(task.getHost());
            taskEntity.setTaskLocality(task.getTaskLocality());
            taskEntity.setSpeculative(task.isSpeculative());

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
        }
    }
}
