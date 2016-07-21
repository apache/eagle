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

package org.apache.eagle.jpm.mr.running.parser;

import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.mr.running.entities.JobConfig;
import org.apache.eagle.jpm.mr.running.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.running.entities.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.mr.running.recover.RunningJobManager;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.jpm.util.resourceFetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourceFetch.connection.URLConnectionUtils;
import org.apache.eagle.jpm.util.resourceFetch.model.*;
import org.apache.eagle.jpm.util.resourceFetch.model.JobCounters;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.*;
import java.util.function.Function;

public class MRJobParser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobParser.class);

    private AppInfo app;
    private static final int MAX_RETRY_TIMES = 3;
    private IEagleServiceClient client;
    private MRJobEntityCreationHandler mrJobEntityCreationHandler;
    //<jobId, JobExecutionAPIEntity>
    private Map<String, JobExecutionAPIEntity> mrJobEntityMap;
    private Map<String, JobConfig> mrJobConfigs;
    private static final String XML_HTTP_HEADER = "Accept";
    private static final String XML_FORMAT = "application/xml";
    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    private MRRunningConfigManager.EndpointConfig endpointConfig;
    private MRRunningConfigManager.JobExtractorConfig jobExtractorConfig;
    private final Object lock = new Object();
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private Map<String, String> commonTags = new HashMap<>();
    private RunningJobManager runningJobManager;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public MRJobParser(MRRunningConfigManager.EagleServiceConfig eagleServiceConfig,
                       MRRunningConfigManager.EndpointConfig endpointConfig,
                       MRRunningConfigManager.JobExtractorConfig jobExtractorConfig,
                       AppInfo app, Map<String, JobExecutionAPIEntity> mrJobMap,
                       RunningJobManager runningJobManager) {
        this.app = app;
        this.mrJobEntityMap = new HashMap<>();
        this.mrJobEntityMap = mrJobMap;
        if (this.mrJobEntityMap == null) {
            this.mrJobEntityMap = new HashMap<>();
        }
        this.mrJobConfigs = new HashMap<>();
        this.client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.mrJobEntityCreationHandler = new MRJobEntityCreationHandler(app.getId(), client);

        this.commonTags.put(MRJobTagName.SITE.toString(), jobExtractorConfig.site);
        this.commonTags.put(MRJobTagName.USER.toString(), app.getUser());
        this.commonTags.put(MRJobTagName.JOB_QUEUE.toString(), app.getQueue());
        this.runningJobManager = runningJobManager;
    }

    private void finishMRJob(String mrJobId) {
        JobExecutionAPIEntity jobExecutionAPIEntity = mrJobEntityMap.get(mrJobId);
        jobExecutionAPIEntity.setStatus(Constants.AppState.FINISHED.toString());
        mrJobConfigs.remove(mrJobId);
        LOG.info("mr job {} has been finished", mrJobId);
    }

    private void fetchMRRunningInfo() throws Exception {
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            if (fetchMRJobs()) {
                break;
            } else if (i >= MAX_RETRY_TIMES - 1) {
                //check whether the app has finished. if we test that we can connect rm, then we consider the jobs have finished
                //if we get here either because of cannot connect rm or the jobs have finished
                new RMResourceFetcher(endpointConfig.rmUrls).getResource(Constants.ResourceType.RUNNING_MR_JOB);
                mrJobEntityMap.keySet().forEach(this::finishMRJob);
            }
            Utils.sleep(5);
        }

        List<Function<String, Boolean>> functions = new ArrayList<>();
        functions.add(fetchJobCounters);
        functions.add(fetchJobConfig);
        functions.add(fetchTasks);
        for (String jobId : mrJobEntityMap.keySet()) {
            for (Function<String, Boolean> function : functions) {
                int i = 0;
                for (; i < MAX_RETRY_TIMES; i++) {
                    if (function.apply(jobId)) {
                        break;
                    }
                    Utils.sleep(5);
                }
                if (i >= MAX_RETRY_TIMES) {
                    //may caused by rm unreachable
                    new RMResourceFetcher(endpointConfig.rmUrls).getResource(Constants.ResourceType.RUNNING_MR_JOB);
                    finishMRJob(jobId);
                    break;
                }
            }
        }
    }

    private boolean fetchMRJobs() {
        String jobURL = app.getTrackingUrl() + Constants.MR_JOBS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        List<MRJob> mrJobs = null;
        try {
            is = InputStreamUtils.getInputStream(jobURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch mr job from {}", jobURL);
            mrJobs = OBJ_MAPPER.readValue(is, MRJobsWrapper.class).getJobs().getJob();
        } catch (Exception e) {
            LOG.warn("fetch mr job from {} failed, {}", jobURL, e);
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        for (MRJob mrJob : mrJobs) {
            String id = mrJob.getId();
            if (!mrJobEntityMap.containsKey(id)) {
                mrJobEntityMap.put(id, new JobExecutionAPIEntity());
            }

            JobExecutionAPIEntity jobExecutionAPIEntity = mrJobEntityMap.get(id);
            jobExecutionAPIEntity.setTags(new HashMap<>(commonTags));
            jobExecutionAPIEntity.getTags().put(MRJobTagName.JOB_ID.toString(), id);
            jobExecutionAPIEntity.getTags().put(MRJobTagName.JOB_NAME.toString(), mrJob.getName());
            jobExecutionAPIEntity.getTags().put(MRJobTagName.NORM_JOB_NAME.toString(), mrJob.getName());
            jobExecutionAPIEntity.setTimestamp(app.getStartedTime());
            jobExecutionAPIEntity.setStartTime(mrJob.getStartTime());
            jobExecutionAPIEntity.setElapsedTime(mrJob.getElapsedTime());
            jobExecutionAPIEntity.setStatus(mrJob.getState());
            jobExecutionAPIEntity.setMapsTotal(mrJob.getMapsTotal());
            jobExecutionAPIEntity.setMapsCompleted(mrJob.getMapsCompleted());
            jobExecutionAPIEntity.setReducesTotal(mrJob.getReducesTotal());
            jobExecutionAPIEntity.setReducesCompleted(mrJob.getReducesCompleted());
            jobExecutionAPIEntity.setMapProgress(mrJob.getMapProgress());
            jobExecutionAPIEntity.setReduceProgress(mrJob.getReduceProgress());
            jobExecutionAPIEntity.setMapsPending(mrJob.getMapsPending());
            jobExecutionAPIEntity.setMapsRunning(mrJob.getMapsRunning());
            jobExecutionAPIEntity.setReducesPending(mrJob.getReducesPending());
            jobExecutionAPIEntity.setReducesRunning(mrJob.getReducesRunning());
            jobExecutionAPIEntity.setNewReduceAttempts(mrJob.getNewReduceAttempts());
            jobExecutionAPIEntity.setRunningReduceAttempts(mrJob.getRunningReduceAttempts());
            jobExecutionAPIEntity.setFailedReduceAttempts(mrJob.getFailedReduceAttempts());
            jobExecutionAPIEntity.setKilledReduceAttempts(mrJob.getKilledReduceAttempts());
            jobExecutionAPIEntity.setSuccessfulReduceAttempts(mrJob.getSuccessfulReduceAttempts());
            jobExecutionAPIEntity.setNewMapAttempts(mrJob.getNewMapAttempts());
            jobExecutionAPIEntity.setRunningMapAttempts(mrJob.getRunningMapAttempts());
            jobExecutionAPIEntity.setFailedMapAttempts(mrJob.getFailedMapAttempts());
            jobExecutionAPIEntity.setKilledMapAttempts(mrJob.getKilledMapAttempts());
            jobExecutionAPIEntity.setSuccessfulMapAttempts(mrJob.getSuccessfulMapAttempts());
            jobExecutionAPIEntity.setAppInfo(app);
            runningJobManager.update(app.getId(), id, jobExecutionAPIEntity);
        }
        return true;
    }

    private Function<String, Boolean> fetchJobCounters = jobId -> {
        String jobCounterURL = app.getTrackingUrl() + Constants.MR_JOBS_URL + "/" + jobId + "/" + Constants.MR_JOB_COUNTERS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        JobCounters jobCounters = null;
        try {
            is = InputStreamUtils.getInputStream(jobCounterURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch mr job counter from {}", jobCounterURL);
            jobCounters = OBJ_MAPPER.readValue(is, JobCountersWrapper.class).getJobCounters();
        } catch (Exception e) {
            LOG.warn("fetch mr job counter from {} failed, {}", jobCounterURL, e);
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        if (jobCounters.getCounterGroup() == null) return true;
        JobExecutionAPIEntity jobExecutionAPIEntity = mrJobEntityMap.get(jobId);
        org.apache.eagle.jpm.util.jobcounter.JobCounters jobCounter = new org.apache.eagle.jpm.util.jobcounter.JobCounters();
        Map<String, Map<String, Long>> groups = new HashMap<>();

        for (JobCounterGroup jobCounterGroup : jobCounters.getCounterGroup()) {
            if (!groups.containsKey(jobCounterGroup.getCounterGroupName())) {
                groups.put(jobCounterGroup.getCounterGroupName(), new HashMap<>());
            }

            Map<String, Long> counterValues = groups.get(jobCounterGroup.getCounterGroupName());
            List<JobCounterItem> items = jobCounterGroup.getCounter();
            if (items == null) return true;
            for (JobCounterItem item : items) {
                counterValues.put(item.getName(), item.getTotalCounterValue());
                //counterValues.put(item.getName() + "_MAP", item.getMapCounterValue());
                //counterValues.put(item.getName() + "_REDUCE", item.getReduceCounterValue());
            }
        }

        jobCounter.setCounters(groups);
        jobExecutionAPIEntity.setJobCounters(jobCounter);
        return true;
    };

    private Function<String, Boolean> fetchTasks = jobId -> {
        String taskURL = app.getTrackingUrl() + Constants.MR_JOBS_URL + "/" + jobId + "/" + Constants.MR_TASKS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        List<MRTask> tasks = null;
        try {
            is = InputStreamUtils.getInputStream(taskURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch mr task from {}", taskURL);
            tasks = OBJ_MAPPER.readValue(is, MRTasksWrapper.class).getTasks().getTask();
        } catch (Exception e) {
            LOG.warn("fetch mr task from {} failed, {}", taskURL, e);
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        for (MRTask task : tasks) {
            TaskExecutionAPIEntity taskExecutionAPIEntity = new TaskExecutionAPIEntity();
            taskExecutionAPIEntity.setTags(new HashMap<>(mrJobEntityMap.get(jobId).getTags()));
            taskExecutionAPIEntity.getTags().put(MRJobTagName.TASK_TYPE.toString(), task.getType());
            taskExecutionAPIEntity.getTags().put(MRJobTagName.TASK_ID.toString(), task.getId());

            taskExecutionAPIEntity.setTimestamp(app.getStartedTime());
            taskExecutionAPIEntity.setStartTime(task.getStartTime());
            taskExecutionAPIEntity.setFinishTime(task.getFinishTime());
            taskExecutionAPIEntity.setElapsedTime(task.getElapsedTime());
            taskExecutionAPIEntity.setProgress(task.getProgress());
            taskExecutionAPIEntity.setStatus(task.getState());
            taskExecutionAPIEntity.setSuccessfulAttempt(task.getSuccessfulAttempt());
            taskExecutionAPIEntity.setStatusDesc(task.getStatus());
            mrJobEntityCreationHandler.add(taskExecutionAPIEntity);
        }
        return true;
    };

    private Function<String, Boolean> fetchJobConfig = jobId -> {
        if (mrJobConfigs.containsKey(jobId)) {
            mrJobEntityMap.get(jobId).setJobConfig(mrJobConfigs.get(jobId));
            return true;
        }
        String confURL = app.getTrackingUrl() + Constants.MR_JOBS_URL + "/" + jobId + "/" + Constants.MR_CONF_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        try {
            LOG.info("fetch job conf from {}", confURL);
            final URLConnection connection = URLConnectionUtils.getConnection(confURL);
            connection.setRequestProperty(XML_HTTP_HEADER, XML_FORMAT);
            connection.setConnectTimeout(CONNECTION_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);
            is = connection.getInputStream();
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dt = db.parse(is);
            Element element = dt.getDocumentElement();
            JobConfig config = new JobConfig();
            NodeList propertyList = element.getElementsByTagName("property");
            int length = propertyList.getLength();
            for (int i = 0; i < length; i++) {
                Node property = propertyList.item(i);
                String key = property.getChildNodes().item(0).getTextContent();
                String value = property.getChildNodes().item(1).getTextContent();
                config.put(key, value);
            }
            mrJobEntityMap.get(jobId).setJobConfig(config);
            mrJobConfigs.put(jobId, config);
        } catch (Exception e) {
            LOG.warn("fetch job conf from {} failed, {}", confURL, e);
            return false;
        } finally {
            Utils.closeInputStream(is);
        }

        return true;
    };

    @Override
    public void run() {
        synchronized (this.lock) {
            LOG.info("start to process yarn application " + app.getId());
            try {
                fetchMRRunningInfo();
            } catch (Exception e) {
                LOG.warn("exception found when process application {}, {}", app.getId(), e);
                e.printStackTrace();
            } finally {
                for (String jobId : mrJobEntityMap.keySet()) {
                    mrJobEntityCreationHandler.add(mrJobEntityMap.get(jobId));
                }
                mrJobEntityCreationHandler.add(null);//force flush
                //delete from zk if needed
                mrJobEntityMap.keySet()
                        .stream()
                        .filter(
                                jobId -> mrJobEntityMap.get(jobId).getStatus().equals(Constants.AppState.FINISHED.toString()) ||
                                        mrJobEntityMap.get(jobId).getStatus().equals(Constants.AppState.FAILED.toString()))
                        .forEach(
                                jobId -> this.runningJobManager.delete(app.getId(), jobId));

                LOG.info("finish process yarn application " + app.getId());
            }
        }
    }
}
