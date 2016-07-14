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

import org.apache.eagle.jpm.spark.running.entities.JobConfig;
import org.apache.eagle.jpm.spark.running.entities.SparkAppEntity;
import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.jpm.spark.running.entities.SparkExecutorEntity;
import org.apache.eagle.jpm.spark.running.entities.SparkJobEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.SparkJobTagName;
import org.apache.eagle.jpm.util.resourceFetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkApplication;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkExecutor;
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

public class SparkApplicationParser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkApplicationParser.class);

    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private AppInfo app;
    private static final int MAX_ENTITIES_SIZE = 10;
    private static final int MAX_RETRY_TIMES = 3;
    private IEagleServiceClient client;

    //<sparkAppId, SparkAppEntity>
    private Map<String, SparkAppEntity> sparkAppEntityMap;
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
        entities.add(entity);
        try {
            if (entities.size() >= MAX_ENTITIES_SIZE) {
                LOG.info("start to flush spark entities for application {}, size {}", app.getId(), entities.size());
                //client.create(entities);
                entities.clear();
                LOG.info("finish flushing spark entities for application {}, size {}", app.getId(), entities.size());
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
    }

    private void fetchSparkRunningInfo() throws Exception {
        //1, fetch Spark Apps
        fetchSparkApps();
        for (String sparkAppId : sparkAppEntityMap.keySet()) {
            //2, fetch Executors
            if (!fetchSparkExecutors(sparkAppId)) { //app finish
                finishSparkApp(sparkAppId);
                continue;
            }
            //3, fetch Jobs
            if (!fetchSparkJobs(sparkAppId)) { //app finish
                finishSparkApp(sparkAppId);
                continue;
            }
            //4, fetch stages and tasks
            if (!fetchSparkStagesAndTasks(sparkAppId)) { //app finish
                finishSparkApp(sparkAppId);
            }
        }
    }

    @Override
    public void run() {
        synchronized (this.lock) {
            LOG.info("start to process yarn application " + app.getId());
            try {
                fetchSparkRunningInfo();
            } catch (Exception e) {
                LOG.warn("exception found when process application {}, {}", app.getId(), e);
            }
        }
    }

    private JobConfig getJobConfig(String sparkAppId, int attemptId) {
        return new JobConfig();
    }

    private void fetchSparkApps() throws Exception {
        InputStream is = null;
        int i = 0;
        while (i < MAX_RETRY_TIMES) {
            String appURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
            try {
                is = InputStreamUtils.getInputStream(appURL, null, Constants.CompressionType.NONE);
                LOG.info("fetch spark application from {}", appURL);
                SparkApplication[] sparkApplications = OBJ_MAPPER.readValue(is, SparkApplication[].class);
                for (SparkApplication sparkApplication : sparkApplications) {
                    String id = sparkApplication.getId();
                    if (id.contains(" ")) {
                        //spark version < 1.6.0 and id contains white space, need research again later
                        LOG.warn("skip spark application {}", id);
                        continue;
                    }

                    currentAttempt = sparkApplication.getAttempts().size();
                    SparkAppEntity attemptEntity;
                    for (int j = 1; j <= currentAttempt; j++) {
                        if (!sparkAppEntityMap.containsKey(id) ||
                                !sparkAppEntityMap.get(id).getTags().get(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString()).equals("" + j)) {
                            //missing attempt
                            attemptEntity = new SparkAppEntity();
                            attemptEntity.setConfig(null);
                            commonTags.put(SparkJobTagName.SPARK_APP_NAME.toString(), sparkApplication.getName());
                            commonTags.put(SparkJobTagName.SPARK_APP_ATTEMPT_ID.toString(), "" + j);
                            commonTags.put(SparkJobTagName.SPARK_APP_ID.toString(), id);
                            attemptEntity.setTags(new HashMap<>(commonTags));
                            attemptEntity.setAppInfo(app);

                            attemptEntity.setStartTime(dateTimeToLong(sparkApplication.getAttempts().get(j - 1).getStartTime()));
                            attemptEntity.setTimestamp(attemptEntity.getStartTime());
                            if (attemptEntity.getConfig() == null) {
                                //read config from hdfs
                                attemptEntity.setConfig(getJobConfig(id, j));
                            }
                        } else {
                            attemptEntity = sparkAppEntityMap.get(id);
                        }
                        if (j == currentAttempt) {
                            //current attempt
                            attemptEntity.setYarnState(app.getState());
                            attemptEntity.setYarnStatus(app.getFinalStatus());
                            sparkAppEntityMap.put(id, attemptEntity);
                        } else {
                            attemptEntity.setYarnState(Constants.AppState.FAILED.toString());
                            attemptEntity.setYarnStatus(Constants.AppStatus.FAILED.toString());
                            flush(attemptEntity);
                        }
                    }
                }
                break;
            } catch (Exception e) {
                i++;
                LOG.warn("fetch spark application from {} failed, {}", appURL, e);
                e.printStackTrace();
            }
        }

        closeInputStream(is);
        if (i >= MAX_RETRY_TIMES) {
            //The application has finished
            throw new Exception();
        }
    }

    private boolean fetchSparkExecutors(String sparkAppId) {
        InputStream is = null;
        int i = 0;
        while (i < MAX_RETRY_TIMES) {
            //only get current attempt
            SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
            String executorURL = "";
            try {
                executorURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_EXECUTORS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
                is = InputStreamUtils.getInputStream(executorURL, null, Constants.CompressionType.NONE);
                LOG.info("fetch spark executor from {}", executorURL);
                SparkExecutor[] sparkExecutors = OBJ_MAPPER.readValue(is, SparkExecutor[].class);
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
                break;
            } catch (Exception e) {
                i++;
                LOG.warn("fetch spark executor from {} failed, {}", executorURL, e);
            }
        }
        closeInputStream(is);

        if (i >= MAX_RETRY_TIMES) {
            return false;
        }
        return true;
    }

    private boolean fetchSparkJobs(String sparkAppId) {
        InputStream is = null;
        int i = 0;
        while (i < MAX_RETRY_TIMES) {
            //only get current attempt
            SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
            String jobURL = "";
            try {
                jobURL = app.getTrackingUrl() + Constants.SPARK_APPS_URL + "/" + sparkAppId + "/" + Constants.SPARK_JOBS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
                is = InputStreamUtils.getInputStream(jobURL, null, Constants.CompressionType.NONE);
                LOG.info("fetch spark job from {}", jobURL);
                break;
            } catch (Exception e) {
                i++;
                LOG.warn("fetch spark job from {} failed, {}", jobURL, e);
            }
        }
        closeInputStream(is);
        if (i >= MAX_RETRY_TIMES) {
            //The application has finished
            return false;
        }
        return true;
    }

    private boolean fetchSparkStagesAndTasks(String sparkAppId) {
        InputStream is = null;
        int i = 0;
        while (i < MAX_RETRY_TIMES) {
            //only get current attempt
            SparkAppEntity sparkAppEntity = sparkAppEntityMap.get(sparkAppId);
            String jobURL = "";
            try {
                break;
            } catch (Exception e) {
                i++;
                LOG.warn("fetch spark job from {} failed, {}", jobURL, e);
            }
        }
        closeInputStream(is);
        if (i >= MAX_RETRY_TIMES) {
            //The application has finished
            return false;
        }
        return true;
    }
}
