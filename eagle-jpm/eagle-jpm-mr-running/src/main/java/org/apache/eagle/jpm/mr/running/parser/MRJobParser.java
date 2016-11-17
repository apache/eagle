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

import com.typesafe.config.Config;
import org.apache.eagle.jpm.mr.running.MRRunningJobConfig;
import org.apache.eagle.jpm.mr.running.recover.MRRunningJobManager;
import org.apache.eagle.jpm.mr.runningentity.JobConfig;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.runningentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.JobNameNormalization;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.connection.URLConnectionUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.*;
import org.apache.eagle.jpm.util.resourcefetch.model.JobCounters;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.io.InputStream;
import java.net.URLConnection;
import java.util.*;
import java.util.function.Function;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class MRJobParser implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobParser.class);

    public enum ParserStatus {
        RUNNING,
        FINISHED,
        APP_FINISHED
    }

    private AppInfo app;
    private static final int MAX_RETRY_TIMES = 2;
    private MRJobEntityCreationHandler mrJobEntityCreationHandler;
    //<jobId, JobExecutionAPIEntity>
    private Map<String, JobExecutionAPIEntity> mrJobEntityMap;
    private Map<String, JobConfig> mrJobConfigs;
    private static final String XML_HTTP_HEADER = "Accept";
    private static final String XML_FORMAT = "application/xml";
    private static final int CONNECTION_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;
    private final Object lock = new Object();
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private Map<String, String> commonTags = new HashMap<>();
    private MRRunningJobManager runningJobManager;
    private ParserStatus parserStatus;
    private ResourceFetcher rmResourceFetcher;
    private Set<String> finishedTaskIds;
    private List<String> configKeys;
    private static final int TOP_BOTTOM_TASKS_BY_ELAPSED_TIME = 10;
    private static final int FLUSH_TASKS_EVERY_TIME = 5;
    private static final int MAX_TASKS_PERMIT = 5000;
    private Config config;

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public MRJobParser(MRRunningJobConfig.EndpointConfig endpointConfig,
                       MRRunningJobConfig.EagleServiceConfig eagleServiceConfig,
                       AppInfo app, Map<String, JobExecutionAPIEntity> mrJobMap,
                       MRRunningJobManager runningJobManager, ResourceFetcher rmResourceFetcher,
                       List<String> configKeys,
                       Config config) {
        this.app = app;
        this.mrJobEntityMap = new HashMap<>();
        this.mrJobEntityMap = mrJobMap;
        if (this.mrJobEntityMap == null) {
            this.mrJobEntityMap = new HashMap<>();
        }
        this.mrJobConfigs = new HashMap<>();

        this.mrJobEntityCreationHandler = new MRJobEntityCreationHandler(eagleServiceConfig);

        this.commonTags.put(MRJobTagName.SITE.toString(), endpointConfig.site);
        this.commonTags.put(MRJobTagName.USER.toString(), app.getUser());
        this.commonTags.put(MRJobTagName.JOB_QUEUE.toString(), app.getQueue());
        this.runningJobManager = runningJobManager;
        this.parserStatus  = ParserStatus.FINISHED;
        this.rmResourceFetcher = rmResourceFetcher;
        this.finishedTaskIds = new HashSet<>();
        this.configKeys = configKeys;
        this.config = config;
    }

    public void setAppInfo(AppInfo app) {
        this.app = app;
    }

    public ParserStatus status() {
        return this.parserStatus;
    }

    public void setStatus(ParserStatus status) {
        this.parserStatus = status;
    }

    private void finishMRJob(String mrJobId) {
        JobExecutionAPIEntity jobExecutionAPIEntity = mrJobEntityMap.get(mrJobId);
        jobExecutionAPIEntity.setInternalState(Constants.AppState.FINISHED.toString());
        jobExecutionAPIEntity.setCurrentState(Constants.AppState.RUNNING.toString());
        mrJobConfigs.remove(mrJobId);
        if (mrJobConfigs.size() == 0) {
            this.parserStatus = ParserStatus.APP_FINISHED;
        }
        LOG.info("mr job {} has been finished", mrJobId);
    }

    private void fetchMRRunningInfo() throws Exception {
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            if (fetchMRJobs()) {
                break;
            } else if (i >= MAX_RETRY_TIMES - 1) {
                //check whether the app has finished. if we test that we can connect rm, then we consider the jobs have finished
                //if we get here either because of cannot connect rm or the jobs have finished
                rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_MR_JOB);
                mrJobEntityMap.keySet().forEach(this::finishMRJob);
                return;
            }
        }

        List<Function<String, Boolean>> functions = new ArrayList<>();
        functions.add(fetchJobConfig);
        functions.add(fetchJobCounters);
        if ((int)(Math.random() * 10) % FLUSH_TASKS_EVERY_TIME == 0) {
            functions.add(fetchTasks);
        }

        for (String jobId : mrJobEntityMap.keySet()) {
            for (Function<String, Boolean> function : functions) {
                int i = 0;
                for (; i < MAX_RETRY_TIMES; i++) {
                    if (function.apply(jobId)) {
                        break;
                    }
                }
                if (i >= MAX_RETRY_TIMES) {
                    //may caused by rm unreachable
                    rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_MR_JOB);
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
            String jobDefId = JobNameNormalization.getInstance(this.config).normalize(mrJob.getName());
            jobExecutionAPIEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobDefId);
            if (mrJobConfigs.get(id) != null) {
                JobConfig jobConfig = mrJobConfigs.get(id);
                if (jobConfig.containsKey(this.configKeys.get(0))) {
                    jobExecutionAPIEntity.getTags().put(MRJobTagName.JOD_DEF_ID.toString(), jobConfig.get(this.configKeys.get(0)));
                }
            }
            jobExecutionAPIEntity.setTimestamp(app.getStartedTime());
            jobExecutionAPIEntity.setSubmissionTime(app.getStartedTime());
            jobExecutionAPIEntity.setTrackingUrl(app.getTrackingUrl());
            jobExecutionAPIEntity.setStartTime(mrJob.getStartTime());
            jobExecutionAPIEntity.setDurationTime(mrJob.getElapsedTime());
            jobExecutionAPIEntity.setCurrentState(mrJob.getState());
            jobExecutionAPIEntity.setInternalState(mrJob.getState());
            jobExecutionAPIEntity.setNumTotalMaps(mrJob.getMapsTotal());
            jobExecutionAPIEntity.setNumFinishedMaps(mrJob.getMapsCompleted());
            jobExecutionAPIEntity.setNumTotalReduces(mrJob.getReducesTotal());
            jobExecutionAPIEntity.setNumFinishedReduces(mrJob.getReducesCompleted());
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
            jobExecutionAPIEntity.setAllocatedMB(app.getAllocatedMB());
            jobExecutionAPIEntity.setAllocatedVCores(app.getAllocatedVCores());
            jobExecutionAPIEntity.setRunningContainers(app.getRunningContainers());
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

        if (jobCounters.getCounterGroup() == null) {
            return true;
        }
        JobExecutionAPIEntity jobExecutionAPIEntity = mrJobEntityMap.get(jobId);
        org.apache.eagle.jpm.util.jobcounter.JobCounters jobCounter = new org.apache.eagle.jpm.util.jobcounter.JobCounters();
        Map<String, Map<String, Long>> groups = new HashMap<>();

        for (JobCounterGroup jobCounterGroup : jobCounters.getCounterGroup()) {
            String counterGroupName = jobCounterGroup.getCounterGroupName();
            if (!groups.containsKey(counterGroupName)) {
                groups.put(counterGroupName, new HashMap<>());
            }

            Map<String, Long> counterValues = groups.get(counterGroupName);
            List<JobCounterItem> items = jobCounterGroup.getCounter();
            if (items == null) {
                continue;
            }
            for (JobCounterItem item : items) {
                String key = item.getName();
                counterValues.put(key, item.getTotalCounterValue());
                if (counterGroupName.equals(Constants.JOB_COUNTER)) {
                    if (key.equals(Constants.JobCounter.DATA_LOCAL_MAPS.toString())) {
                        jobExecutionAPIEntity.setDataLocalMaps((int)item.getTotalCounterValue());
                    } else if (key.equals(Constants.JobCounter.RACK_LOCAL_MAPS.toString())) {
                        jobExecutionAPIEntity.setRackLocalMaps((int)item.getTotalCounterValue());
                    } else if (key.equals(Constants.JobCounter.TOTAL_LAUNCHED_MAPS.toString())) {
                        jobExecutionAPIEntity.setTotalLaunchedMaps((int)item.getTotalCounterValue());
                    }
                }
            }
        }

        jobCounter.setCounters(groups);
        jobExecutionAPIEntity.setJobCounters(jobCounter);
        if (jobExecutionAPIEntity.getTotalLaunchedMaps() > 0) {
            jobExecutionAPIEntity.setDataLocalMapsPercentage(jobExecutionAPIEntity.getDataLocalMaps() * 1.0 / jobExecutionAPIEntity.getTotalLaunchedMaps());
            jobExecutionAPIEntity.setRackLocalMapsPercentage(jobExecutionAPIEntity.getRackLocalMaps() * 1.0 / jobExecutionAPIEntity.getTotalLaunchedMaps());
        }
        return true;
    };

    private Function<Pair<String, String>, org.apache.eagle.jpm.util.jobcounter.JobCounters> fetchTaskCounters = jobAndTaskId -> {
        org.apache.eagle.jpm.util.jobcounter.JobCounters jobCounter = new org.apache.eagle.jpm.util.jobcounter.JobCounters();
        String jobId = jobAndTaskId.getLeft();
        String taskId = jobAndTaskId.getRight();
        String taskCounterURL = app.getTrackingUrl()
            + Constants.MR_JOBS_URL + "/"
            + jobId + "/" + Constants.MR_TASKS_URL + "/"
            + taskId + "/" + Constants.MR_JOB_COUNTERS_URL
            + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        TaskCounters taskCounters = null;
        try {
            is = InputStreamUtils.getInputStream(taskCounterURL, null, Constants.CompressionType.NONE);
            LOG.info("fetch mr task counters from {}", taskCounterURL);
            taskCounters = OBJ_MAPPER.readValue(is, TaskCountersWrapper.class).getJobTaskCounters();
        } catch (Exception e) {
            LOG.warn("fetch mr task counters from {} failed, {}", taskCounterURL, e);
            return null;
        } finally {
            Utils.closeInputStream(is);
        }

        if (taskCounters.getTaskCounterGroup() == null) {
            return jobCounter;
        }
        Map<String, Map<String, Long>> groups = new HashMap<>();

        for (TaskCounterGroup taskCounterGroup : taskCounters.getTaskCounterGroup()) {
            if (!groups.containsKey(taskCounterGroup.getCounterGroupName())) {
                groups.put(taskCounterGroup.getCounterGroupName(), new HashMap<>());
            }

            Map<String, Long> counterValues = groups.get(taskCounterGroup.getCounterGroupName());
            List<TaskCounterItem> items = taskCounterGroup.getCounter();
            if (items == null) {
                continue;
            }
            for (TaskCounterItem item : items) {
                counterValues.put(item.getName(), item.getValue());
            }
        }

        jobCounter.setCounters(groups);

        return jobCounter;
    };

    private Function<Pair<String, String>, TaskAttemptExecutionAPIEntity> fetchTaskAttempt = jobAndTaskId -> {
        String jobId = jobAndTaskId.getLeft();
        String taskId = jobAndTaskId.getRight();
        String taskAttemptURL = app.getTrackingUrl()
            + Constants.MR_JOBS_URL + "/"
            + jobId + "/" + Constants.MR_TASKS_URL + "/"
            + taskId + "/" + Constants.MR_TASK_ATTEMPTS_URL + "?" + Constants.ANONYMOUS_PARAMETER;
        InputStream is = null;
        List<MRTaskAttempt> taskAttempts = null;
        try {
            is = InputStreamUtils.getInputStream(taskAttemptURL, null, Constants.CompressionType.GZIP);
            LOG.info("fetch mr task attempt from {}", taskAttemptURL);
            taskAttempts = OBJ_MAPPER.readValue(is, MRTaskAttemptWrapper.class).getTaskAttempts().getTaskAttempt();
        } catch (Exception e) {
            LOG.warn("fetch mr task attempt from {} failed, {}", taskAttemptURL, e);
            return null;
        } finally {
            Utils.closeInputStream(is);
        }
        Comparator<MRTaskAttempt> byStartTime = (e1, e2) -> -1 * Long.compare(e1.getStartTime(), e2.getStartTime());
        Iterator<MRTaskAttempt> taskAttemptIterator = taskAttempts.stream().sorted(byStartTime).iterator();
        while (taskAttemptIterator.hasNext()) {
            MRTaskAttempt mrTaskAttempt = taskAttemptIterator.next();
            TaskAttemptExecutionAPIEntity taskAttemptExecutionAPIEntity = new TaskAttemptExecutionAPIEntity();
            taskAttemptExecutionAPIEntity.setTags(new HashMap<>(mrJobEntityMap.get(jobId).getTags()));
            taskAttemptExecutionAPIEntity.getTags().put(MRJobTagName.TASK_TYPE.toString(), mrTaskAttempt.getType());
            taskAttemptExecutionAPIEntity.getTags().put(MRJobTagName.TASK_ID.toString(), taskId);
            taskAttemptExecutionAPIEntity.getTags().put(MRJobTagName.RACK.toString(), mrTaskAttempt.getRack());
            String nodeHttpAddress = mrTaskAttempt.getNodeHttpAddress();
            String host = nodeHttpAddress.substring(0, nodeHttpAddress.indexOf(':'));
            taskAttemptExecutionAPIEntity.getTags().put(MRJobTagName.HOSTNAME.toString(), host);

            taskAttemptExecutionAPIEntity.setTimestamp(mrTaskAttempt.getStartTime());
            taskAttemptExecutionAPIEntity.setStartTime(mrTaskAttempt.getStartTime());
            taskAttemptExecutionAPIEntity.setFinishTime(mrTaskAttempt.getFinishTime());
            taskAttemptExecutionAPIEntity.setElapsedTime(mrTaskAttempt.getElapsedTime());
            taskAttemptExecutionAPIEntity.setProgress(mrTaskAttempt.getProgress());
            taskAttemptExecutionAPIEntity.setId(mrTaskAttempt.getId());
            taskAttemptExecutionAPIEntity.setStatus(mrTaskAttempt.getState());
            taskAttemptExecutionAPIEntity.setDiagnostics(mrTaskAttempt.getDiagnostics());
            taskAttemptExecutionAPIEntity.setType(mrTaskAttempt.getType());
            taskAttemptExecutionAPIEntity.setAssignedContainerId(mrTaskAttempt.getAssignedContainerId());
            this.mrJobEntityCreationHandler.add(taskAttemptExecutionAPIEntity);
            return taskAttemptExecutionAPIEntity;
        }
        return null;
    };

    private void needFetchAttemptTasks(Iterator<MRTask> taskIterator, Set<String> needFetchAttemptTasks) {
        int i = 0;
        while (taskIterator.hasNext() && i < TOP_BOTTOM_TASKS_BY_ELAPSED_TIME) {
            MRTask mrTask = taskIterator.next();
            if (mrTask.getElapsedTime() > 0) {
                i++;
                needFetchAttemptTasks.add(mrTask.getId());
            }
        }
    }

    private Set<String> calcFetchCounterAndAttemptTaskId(List<MRTask> tasks) {
        Set<String> needFetchAttemptTasks = new HashSet<>();
        //1, sort by elapsedTime
        Comparator<MRTask> byElapsedTimeIncrease = (e1, e2) -> Long.compare(e1.getElapsedTime(), e2.getElapsedTime());
        Comparator<MRTask> byElapsedTimeDecrease = (e1, e2) -> -1 * Long.compare(e1.getElapsedTime(), e2.getElapsedTime());
        //2, get finished bottom n
        Iterator<MRTask> taskIteratorIncrease = tasks.stream()
            .filter(task -> task.getState().equals(Constants.TaskState.SUCCEEDED.toString()))
            .sorted(byElapsedTimeIncrease).iterator();
        needFetchAttemptTasks(taskIteratorIncrease, needFetchAttemptTasks);

        //3, fetch finished top n
        Iterator<MRTask> taskIteratorDecrease = tasks.stream()
            .filter(task -> task.getState().equals(Constants.TaskState.SUCCEEDED.toString()))
            .sorted(byElapsedTimeDecrease).iterator();
        needFetchAttemptTasks(taskIteratorDecrease, needFetchAttemptTasks);

        //4, fetch running top n
        taskIteratorDecrease = tasks.stream()
            .filter(task -> task.getState().equals(Constants.TaskState.RUNNING.toString()))
            .sorted(byElapsedTimeDecrease).iterator();
        needFetchAttemptTasks(taskIteratorDecrease, needFetchAttemptTasks);

        return needFetchAttemptTasks;
    }

    private Function<String, Boolean> fetchTasks = jobId -> {
        try {
            JobExecutionAPIEntity entity = this.mrJobEntityMap.get(jobId);
            int taskNumber = entity.getNumTotalMaps() + entity.getNumTotalReduces();
            if (taskNumber > MAX_TASKS_PERMIT) {
                LOG.info("too many tasks {}, ignore tasks", taskNumber);
                return true;
            }
        } catch (Exception e) {
            return true;
        }
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

        Set<String> needFetchAttemptTasks = new HashSet<>();//calcFetchCounterAndAttemptTaskId(tasks);
        for (MRTask task : tasks) {
            if (this.finishedTaskIds.contains(task.getId()) && !needFetchAttemptTasks.contains(task.getId())) {
                continue;
            }
            TaskExecutionAPIEntity taskExecutionAPIEntity = new TaskExecutionAPIEntity();
            taskExecutionAPIEntity.setTags(new HashMap<>(mrJobEntityMap.get(jobId).getTags()));
            taskExecutionAPIEntity.getTags().put(MRJobTagName.TASK_TYPE.toString(), task.getType());
            taskExecutionAPIEntity.getTags().put(MRJobTagName.TASK_ID.toString(), task.getId());

            taskExecutionAPIEntity.setTimestamp(app.getStartedTime());
            taskExecutionAPIEntity.setStartTime(task.getStartTime());
            taskExecutionAPIEntity.setEndTime(task.getFinishTime());
            taskExecutionAPIEntity.setDuration(task.getElapsedTime());
            taskExecutionAPIEntity.setProgress(task.getProgress());
            taskExecutionAPIEntity.setTaskStatus(task.getState());
            taskExecutionAPIEntity.setSuccessfulAttempt(task.getSuccessfulAttempt());
            taskExecutionAPIEntity.setStatusDesc(task.getStatus());

            if (needFetchAttemptTasks.contains(task.getId())) {
                taskExecutionAPIEntity.setJobCounters(fetchTaskCounters.apply(Pair.of(jobId, task.getId())));
                if (taskExecutionAPIEntity.getJobCounters() == null) {
                    return false;
                }

                TaskAttemptExecutionAPIEntity taskAttemptExecutionAPIEntity = fetchTaskAttempt.apply(Pair.of(jobId, task.getId()));
                if (taskAttemptExecutionAPIEntity != null) {
                    taskExecutionAPIEntity.getTags().put(MRJobTagName.HOSTNAME.toString(), taskAttemptExecutionAPIEntity.getTags().get(MRJobTagName.HOSTNAME.toString()));
                    //taskExecutionAPIEntity.setHost(taskAttemptExecutionAPIEntity.getTags().get(MRJobTagName.HOSTNAME.toString()));
                }
            }

            mrJobEntityCreationHandler.add(taskExecutionAPIEntity);

            if (task.getState().equals(Constants.TaskState.SUCCEEDED.toString())
                || task.getState().equals(Constants.TaskState.FAILED.toString())
                || task.getState().equals(Constants.TaskState.KILLED.toString())
                || task.getState().equals(Constants.TaskState.KILL_WAIT.toString())) {
                //LOG.info("mr job {} task {} has finished", jobId, task.getId());
                this.finishedTaskIds.add(task.getId());
            }
        }
        return true;
    };

    private Function<String, Boolean> fetchJobConfig = jobId -> {
        if (mrJobConfigs.containsKey(jobId)) {
            mrJobEntityMap.get(jobId).setJobConfig(mrJobConfigs.get(jobId));
            mrJobEntityMap.get(jobId).getTags().put(MRJobTagName.JOB_TYPE.toString(), Utils.fetchJobType(mrJobConfigs.get(jobId)).toString());
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
                if (this.configKeys.contains(key)) {
                    config.put(key, value);
                }

                if (!this.configKeys.isEmpty() && key.equals(this.configKeys.get(0))) {
                    mrJobEntityMap.get(jobId).getTags().put(MRJobTagName.JOD_DEF_ID.toString(), value);
                }
            }
            mrJobEntityMap.get(jobId).getTags().put(MRJobTagName.JOB_TYPE.toString(), Utils.fetchJobType(config).toString());
            mrJobEntityMap.get(jobId).setJobConfig(config);
            mrJobConfigs.put(jobId, config);

            mrJobEntityCreationHandler.add(mrJobEntityMap.get(jobId));
            runningJobManager.update(app.getId(), jobId, mrJobEntityMap.get(jobId));
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
            if (this.parserStatus == ParserStatus.APP_FINISHED) {
                return;
            }

            this.parserStatus = ParserStatus.RUNNING;
            LOG.info("start to process yarn application " + app.getId());
            try {
                fetchMRRunningInfo();
            } catch (Exception e) {
                LOG.warn("exception found when process application {}, {}", app.getId(), e);
            } finally {
                for (String jobId : mrJobEntityMap.keySet()) {
                    mrJobEntityCreationHandler.add(mrJobEntityMap.get(jobId));
                }
                if (mrJobEntityCreationHandler.flush()) { //force flush
                    //we must flush entities before delete from zk in case of missing finish state of jobs
                    //delete from zk if needed
                    mrJobEntityMap.keySet()
                        .stream()
                        .filter(
                            jobId -> mrJobEntityMap.get(jobId).getInternalState().equals(Constants.AppState.FINISHED.toString())
                                || mrJobEntityMap.get(jobId).getInternalState().equals(Constants.AppState.FAILED.toString()))
                        .forEach(
                            jobId -> this.runningJobManager.delete(app.getId(), jobId));
                }

                LOG.info("finish process yarn application " + app.getId());
            }
            if (this.parserStatus == ParserStatus.RUNNING) {
                this.parserStatus = ParserStatus.FINISHED;
            }
        }
    }
}