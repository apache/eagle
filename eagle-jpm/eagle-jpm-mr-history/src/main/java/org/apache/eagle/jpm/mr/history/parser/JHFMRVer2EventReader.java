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

import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.parser.JHFMRVer1Parser.Keys;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JHFMRVer2EventReader extends JHFEventReaderBase {
    private static final Logger logger = LoggerFactory.getLogger(JHFMRVer2EventReader.class);

    /**
     * Create a new Event Reader.
     *
     * @throws IOException
     */
    public JHFMRVer2EventReader(Map<String, String> baseTags, Configuration configuration, JobHistoryContentFilter filter) {
        super(baseTags, configuration, filter);
    }

    @SuppressWarnings("deprecation")
    public void handleEvent(Event wrapper) throws Exception {
        switch (wrapper.type) {
            case JOB_SUBMITTED:
                handleJobSubmitted(wrapper);
                break;
            case JOB_INITED:
                handleJobInited(wrapper);
                break;
            case JOB_FINISHED:
                handleJobFinished(wrapper);
                break;
            case JOB_PRIORITY_CHANGED:
                handleJobPriorityChanged();
                break;
            case JOB_STATUS_CHANGED:
                handleJobStatusChanged();
                break;
            case JOB_FAILED:
            case JOB_KILLED:
            case JOB_ERROR:
                handleJobUnsuccessfulCompletion(wrapper);
                break;
            case JOB_INFO_CHANGED:
                handleJobInfoChanged();
                break;
            case JOB_QUEUE_CHANGED:
                handleJobQueueChanged();
                break;
            case TASK_STARTED:
                handleTaskStarted(wrapper);
                break;
            case TASK_FINISHED:
                handleTaskFinished(wrapper);
                break;
            case TASK_FAILED:
                handleTaskFailed(wrapper);
                break;
            case TASK_UPDATED:
                handleTaskUpdated();
                break;

            // map task
            case MAP_ATTEMPT_STARTED:
                handleMapAttemptStarted(wrapper);
                break;
            case MAP_ATTEMPT_FINISHED:
                handleMapAttemptFinished(wrapper);
                break;
            case MAP_ATTEMPT_FAILED:
                handleMapAttemptFailed(wrapper);
                break;
            case MAP_ATTEMPT_KILLED:
                handleMapAttemptKilled(wrapper);
                break;

            // reduce task
            case REDUCE_ATTEMPT_STARTED:
                handleReduceAttemptStarted(wrapper);
                break;
            case REDUCE_ATTEMPT_FINISHED:
                handleReduceAttemptFinished(wrapper);
                break;
            case REDUCE_ATTEMPT_FAILED:
                handleReduceAttemptFailed(wrapper);
                break;
            case REDUCE_ATTEMPT_KILLED:
                handleReduceAttemptKilled(wrapper);
                break;

            // set up task
            case SETUP_ATTEMPT_STARTED:
                break;
            case SETUP_ATTEMPT_FINISHED:
                handleSetupAttemptFinished();
                break;
            case SETUP_ATTEMPT_FAILED:
                handleSetupAttemptFailed();
                break;
            case SETUP_ATTEMPT_KILLED:
                handleSetupAttemptKilled();
                break;

            // clean up task
            case CLEANUP_ATTEMPT_STARTED:
                break;
            case CLEANUP_ATTEMPT_FINISHED:
                handleCleanupAttemptFinished();
                break;
            case CLEANUP_ATTEMPT_FAILED:
                handleCleanupAttemptFailed();
                break;
            case CLEANUP_ATTEMPT_KILLED:
                handleCleanupAttemptKilled();
                break;

            case AM_STARTED:
                handleAMStarted();
                break;
            default:
                logger.warn("unexpected event type: " + wrapper.type);
        }
    }

    private void handleJobPriorityChanged() throws Exception {
        return;
    }

    private void handleJobStatusChanged() throws Exception {
        return;
    }

    private void handleJobInfoChanged() throws Exception {
        return;
    }

    private void handleJobQueueChanged() throws Exception {
        return;
    }

    private void handleJobSubmitted(Event wrapper) throws Exception {
        JobSubmitted js = ((JobSubmitted) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getJobid() != null) {
            values.put(Keys.JOBID, js.getJobid().toString());
        }
        if (js.getJobName() != null) {
            values.put(Keys.JOBNAME, js.getJobName().toString());
        }
        if (js.getUserName() != null) {
            values.put(Keys.USER, js.getUserName().toString());
        }
        if (js.getSubmitTime() != null) {
            values.put(Keys.SUBMIT_TIME, js.getSubmitTime().toString());
        }
        if (js.getJobQueueName() != null) {
            values.put(Keys.JOB_QUEUE, js.getJobQueueName().toString());
        }
        handleJob(wrapper.getType(), values, null);
    }

    private void handleJobInited(Event wrapper) throws Exception {
        JobInited js = ((JobInited) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getJobid() != null) {
            values.put(Keys.JOBID, js.getJobid().toString());
        }
        if (js.getLaunchTime() != null) {
            values.put(Keys.LAUNCH_TIME, js.getLaunchTime().toString());
        }
        if (js.getTotalMaps() != null) {
            values.put(Keys.TOTAL_MAPS, js.getTotalMaps().toString());
        }
        if (js.getTotalReduces() != null) {
            values.put(Keys.TOTAL_REDUCES, js.getTotalReduces().toString());
        }
        if (js.getJobStatus() != null) {
            values.put(Keys.JOB_STATUS, js.getJobStatus().toString());
        }
        if (js.getUberized() != null) {
            values.put(Keys.UBERISED, js.getUberized().toString());
        }
        handleJob(wrapper.getType(), values, null);
    }

    private void handleJobFinished(Event wrapper) throws Exception {
        JobFinished js = ((JobFinished) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getJobid() != null) {
            values.put(Keys.JOBID, js.getJobid().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getFinishedMaps() != null) {
            values.put(Keys.FINISHED_MAPS, js.getFinishedMaps().toString());
        }
        if (js.getFinishedReduces() != null) {
            values.put(Keys.FINISHED_REDUCES, js.getFinishedReduces().toString());
        }
        if (js.getFailedMaps() != null) {
            values.put(Keys.FAILED_MAPS, js.getFailedMaps().toString());
        }
        if (js.getFailedReduces() != null) {
            values.put(Keys.FAILED_REDUCES, js.getFailedReduces().toString());
        }
        values.put(Keys.JOB_STATUS, EagleJobStatus.SUCCESS.name());
        handleJob(wrapper.getType(), values, js.getTotalCounters());
    }

    private void handleJobUnsuccessfulCompletion(Event wrapper) throws Exception {
        JobUnsuccessfulCompletion js = ((JobUnsuccessfulCompletion) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getJobid() != null) {
            values.put(Keys.JOBID, js.getJobid().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getFinishedMaps() != null) {
            values.put(Keys.FINISHED_MAPS, js.getFinishedMaps().toString());
        }
        if (js.getFinishedReduces() != null) {
            values.put(Keys.FINISHED_REDUCES, js.getFinishedReduces().toString());
        }
        if (js.getJobStatus() != null) {
            values.put(Keys.JOB_STATUS, js.getJobStatus().toString());
        }
        handleJob(wrapper.getType(), values, null);
    }

    private void handleTaskStarted(Event wrapper) throws Exception {
        TaskStarted js = ((TaskStarted) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getStartTime() != null) {
            values.put(Keys.START_TIME, js.getStartTime().toString());
        }
        if (js.getSplitLocations() != null) {
            values.put(Keys.SPLIT_LOCATIONS, js.getSplitLocations().toString());
        }
        handleTask(RecordTypes.Task, wrapper.getType(), values, null);
    }

    private void handleTaskFinished(Event wrapper) throws Exception {
        TaskFinished js = ((TaskFinished) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getStatus() != null) {
            values.put(Keys.TASK_STATUS, normalizeTaskStatus(js.getStatus().toString()));
        }
        handleTask(RecordTypes.Task, wrapper.getType(), values, js.getCounters());
    }

    private void handleTaskFailed(Event wrapper) throws Exception {
        TaskFailed js = ((TaskFailed) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();

        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getStatus() != null) {
            values.put(Keys.TASK_STATUS, normalizeTaskStatus(js.getStatus().toString()));
        }
        if (js.getError() != null) {
            values.put(Keys.ERROR, js.getError().toString());
        }
        if (js.getFailedDueToAttempt() != null) {
            values.put(Keys.FAILED_DUE_TO_ATTEMPT, js.getFailedDueToAttempt().toString());
        }
        handleTask(RecordTypes.Task, wrapper.getType(), values, js.getCounters());
    }

    private String normalizeTaskStatus(String taskStatus) {
        if (taskStatus.equals("SUCCEEDED")) {
            return EagleTaskStatus.SUCCESS.name();
        }
        return taskStatus;
    }

    private void handleTaskUpdated() {
        return;
    }

    private void handleMapAttemptStarted(Event wrapper) throws Exception {
        handleTaskAttemptStarted(wrapper, RecordTypes.MapAttempt);
    }

    private void handleReduceAttemptStarted(Event wrapper) throws Exception {
        handleTaskAttemptStarted(wrapper, RecordTypes.ReduceAttempt);
    }

    private void handleTaskAttemptStarted(Event wrapper, RecordTypes recordType) throws Exception {
        TaskAttemptStarted js = ((TaskAttemptStarted) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getAttemptId() != null) {
            values.put(Keys.TASK_ATTEMPT_ID, js.getAttemptId().toString());
        }
        if (js.getStartTime() != null) {
            values.put(Keys.START_TIME, js.getStartTime().toString());
        }
        if (js.getTrackerName() != null) {
            values.put(Keys.TRACKER_NAME, js.getTrackerName().toString());
        }
        if (js.getHttpPort() != null) {
            values.put(Keys.HTTP_PORT, js.getHttpPort().toString());
        }
        if (js.getShufflePort() != null) {
            values.put(Keys.SHUFFLE_PORT, js.getShufflePort().toString());
        }
        if (js.getLocality() != null) {
            values.put(Keys.LOCALITY, js.getLocality().toString());
        }
        if (js.getAvataar() != null) {
            values.put(Keys.AVATAAR, js.getAvataar().toString());
        }
        handleTask(recordType, wrapper.getType(), values, null);
    }

    private void handleMapAttemptFinished(Event wrapper) throws Exception {
        MapAttemptFinished js = ((MapAttemptFinished) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getTaskStatus() != null) {
            values.put(Keys.TASK_STATUS, normalizeTaskStatus(js.getTaskStatus().toString()));
        }
        if (js.getAttemptId() != null) {
            values.put(Keys.TASK_ATTEMPT_ID, js.getAttemptId().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getMapFinishTime() != null) {
            values.put(Keys.MAP_FINISH_TIME, js.getMapFinishTime().toString());
        }
        if (js.getHostname() != null) {
            values.put(Keys.HOSTNAME, js.getHostname().toString());
        }
        if (js.getPort() != null) {
            values.put(Keys.PORT, js.getPort().toString());
        }
        if (js.getRackname() != null) {
            values.put(Keys.RACK_NAME, js.getRackname().toString());
        }
        if (js.getState() != null) {
            values.put(Keys.STATE_STRING, js.getState().toString());
        }

        ensureRackAfterAttemptFinish(js.getRackname().toString(), values);
        handleTask(RecordTypes.MapAttempt, wrapper.getType(), values, js.getCounters());
    }

    private void handleReduceAttemptFinished(Event wrapper) throws Exception {
        ReduceAttemptFinished js = ((ReduceAttemptFinished) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getTaskStatus() != null) {
            values.put(Keys.TASK_STATUS, normalizeTaskStatus(js.getTaskStatus().toString()));
        }
        if (js.getAttemptId() != null) {
            values.put(Keys.TASK_ATTEMPT_ID, js.getAttemptId().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getShuffleFinishTime() != null) {
            values.put(Keys.SHUFFLE_FINISHED, js.getShuffleFinishTime().toString());
        }
        if (js.getSortFinishTime() != null) {
            values.put(Keys.SORT_FINISHED, js.getSortFinishTime().toString());
        }
        if (js.getHostname() != null) {
            values.put(Keys.HOSTNAME, js.getHostname().toString());
        }
        if (js.getPort() != null) {
            values.put(Keys.PORT, js.getPort().toString());
        }
        if (js.getRackname() != null) {
            values.put(Keys.RACK_NAME, js.getRackname().toString());
        }
        if (js.getState() != null) {
            values.put(Keys.STATE_STRING, js.getState().toString());
        }
        ensureRackAfterAttemptFinish(js.getRackname().toString(), values);
        handleTask(RecordTypes.ReduceAttempt, wrapper.getType(), values, js.getCounters());
    }

    private void ensureRackAfterAttemptFinish(String rackname, Map<Keys, String> values) {
        // rack name has the format like /default/rack13
        String[] tmp = rackname.split("/");
        String rack = tmp[tmp.length - 1];
        values.put(Keys.RACK, rack);
        host2RackMapping.put(values.get(Keys.HOSTNAME), rack);
    }

    private void handleMapAttemptFailed(Event wrapper) throws Exception {
        handleTaskAttemptFailed(wrapper, RecordTypes.MapAttempt);
    }

    private void handleReduceAttemptFailed(Event wrapper) throws Exception {
        handleTaskAttemptFailed(wrapper, RecordTypes.ReduceAttempt);
    }

    private void handleMapAttemptKilled(Event wrapper) throws Exception {
        handleTaskAttemptFailed(wrapper, RecordTypes.MapAttempt);
    }

    private void handleReduceAttemptKilled(Event wrapper) throws Exception {
        handleTaskAttemptFailed(wrapper, RecordTypes.ReduceAttempt);
    }

    private void handleTaskAttemptFailed(Event wrapper, RecordTypes recordType) throws Exception {
        TaskAttemptUnsuccessfulCompletion js = ((TaskAttemptUnsuccessfulCompletion) wrapper.getEvent());
        Map<Keys, String> values = new HashMap<>();
        if (js.getTaskid() != null) {
            values.put(Keys.TASKID, js.getTaskid().toString());
        }
        if (js.getTaskType() != null) {
            values.put(Keys.TASK_TYPE, js.getTaskType().toString());
        }
        if (js.getAttemptId() != null) {
            values.put(Keys.TASK_ATTEMPT_ID, js.getAttemptId().toString());
        }
        if (js.getFinishTime() != null) {
            values.put(Keys.FINISH_TIME, js.getFinishTime().toString());
        }
        if (js.getHostname() != null) {
            values.put(Keys.HOSTNAME, js.getHostname().toString());
        }
        if (js.getPort() != null) {
            values.put(Keys.PORT, js.getPort().toString());
        }
        if (js.getRackname() != null) {
            values.put(Keys.RACK_NAME, js.getRackname().toString());
        }
        if (js.getError() != null) {
            values.put(Keys.ERROR, js.getError().toString());
        }
        if (js.getStatus() != null) {
            values.put(Keys.TASK_STATUS, js.getStatus().toString());
        }
        ensureRackAfterAttemptFinish(js.getRackname().toString(), values);
        handleTask(recordType, wrapper.getType(), values, js.getCounters());
    }

    private void handleSetupAttemptFinished() {
        return;
    }

    private void handleSetupAttemptFailed() {
        return;
    }

    private void handleSetupAttemptKilled() {
        return;
    }

    private void handleCleanupAttemptFinished() {
        return;
    }

    private void handleCleanupAttemptFailed() {
        return;
    }

    private void handleCleanupAttemptKilled() {
        return;
    }

    private void handleAMStarted() {
        return;
    }

    protected JobCounters parseCounters(Object value) throws IOException {
        JobCounters jc = new JobCounters();
        Map<String, Map<String, Long>> groups = new HashMap<>();
        JhCounters counters = (JhCounters) value;
        List<JhCounterGroup> list = counters.getGroups();
        for (JhCounterGroup cg : list) {
            String cgName = cg.getName().toString();
            if (!cgName.equals("org.apache.hadoop.mapreduce.FileSystemCounter")
                && !cgName.equals("org.apache.hadoop.mapreduce.TaskCounter")
                && !cgName.equals("org.apache.hadoop.mapreduce.JobCounter")
                && !cgName.equals("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter")
                && !cgName.equals("org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter")
                && !cgName.equals("FileSystemCounters")                                           // for artemis
                && !cgName.equals("org.apache.hadoop.mapred.Task$Counter")                        // for artemis
                && !cgName.equals("org.apache.hadoop.mapreduce.lib.input.FileInputFormat$Counter") // for artemis
                && !cgName.equals("org.apache.hadoop.mapreduce.lib.input.FileOutputFormat$Counter") // for artemis
                ) {
                continue;
            }

            groups.put(cgName, new HashMap<String, Long>());
            Map<String, Long> counterValues = groups.get(cgName);
            logger.debug("groupname:" + cg.getName() + "(" + cg.getDisplayName() + ")");

            for (JhCounter c : cg.getCounts()) {
                counterValues.put(c.getName().toString(), c.getValue());
                logger.debug(c.getName() + "=" + c.getValue() + "(" + c.getDisplayName() + ")");
            }
        }
        jc.setCounters(groups);
        return jc;
    }
}

