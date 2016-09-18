/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.service.jpm;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;

import org.apache.eagle.service.jpm.count.MRTaskCountImpl;
import org.apache.eagle.service.jpm.suggestion.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.eagle.jpm.util.MRJobTagName.TASK_TYPE;

@Path("mrTasks")
public class MRTaskExecutionResource {
    private static final Logger LOG = LoggerFactory.getLogger(MRTaskExecutionResource.class);
    MRTaskCountImpl taskCountImpl = new MRTaskCountImpl();

    @GET
    @Path("taskCountsByDuration")
    @Produces(MediaType.APPLICATION_JSON)
    public MRJobTaskCountResponse.TaskCountPerJobResponse getTaskCountsGroupByDuration(@QueryParam("site") String site,
                                                                                       @QueryParam("jobId") String jobId,
                                                                                       @QueryParam("jobStartTime") String jobStartTime,
                                                                                       @QueryParam("jobEndTime") String jobEndTime,
                                                                                       @QueryParam("timeDistInSecs") String timeDistInSecs,
                                                                                       @QueryParam("top") long top) {
        MRJobTaskCountResponse.TaskCountPerJobResponse response = new MRJobTaskCountResponse.TaskCountPerJobResponse();
        if (jobId == null || site == null || timeDistInSecs == null || timeDistInSecs.isEmpty()) {
            response.errMessage = "IllegalArgumentException: jobId == null || site == null || timeDistInSecs == null or isEmpty";
            return response;
        }
        List<MRJobTaskCountResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        List<MRJobTaskCountResponse.UnitTaskCount> finishedTaskCount = new ArrayList<>();

        List<Long> times = ResourceUtils.parseDistributionList(timeDistInSecs);
        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, jobId);
        GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> historyRes =
            ResourceUtils.getQueryResult(query, jobStartTime, jobEndTime);
        if (historyRes.isSuccess() && historyRes.getObj() != null && historyRes.getObj().size() > 0) {
            taskCountImpl.initTaskCountList(runningTaskCount, finishedTaskCount, times, new MRTaskCountImpl.HistoryTaskComparator());
            for (org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o : historyRes.getObj()) {
                int index = ResourceUtils.getDistributionPosition(times, o.getDuration() / DateTimeUtil.ONESECOND);
                MRJobTaskCountResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                taskCountImpl.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                counter.entities.add(o);
            }
        } else {
            query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_RUNNING_TASK_EXECUTION_SERVICE_NAME, site, jobId);
            GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity> runningRes =
                ResourceUtils.getQueryResult(query, jobStartTime, jobEndTime);
            if (runningRes.isSuccess() && runningRes.getObj() != null) {
                taskCountImpl.initTaskCountList(runningTaskCount, finishedTaskCount, times, new MRTaskCountImpl.RunningTaskComparator());
                for (org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity o : runningRes.getObj()) {
                    int index = ResourceUtils.getDistributionPosition(times, o.getDuration() / DateTimeUtil.ONESECOND);
                    if (o.getTaskStatus().equalsIgnoreCase(Constants.TaskState.RUNNING.toString())) {
                        MRJobTaskCountResponse.UnitTaskCount counter = runningTaskCount.get(index);
                        taskCountImpl.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                        counter.entities.add(o);
                    } else if (o.getEndTime() != 0) {
                        MRJobTaskCountResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                        taskCountImpl.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                        counter.entities.add(o);
                    }
                }
            }
        }
        if (top > 0) {
            taskCountImpl.getTopTasks(runningTaskCount, top);
            response.runningTaskCount = runningTaskCount;
            taskCountImpl.getTopTasks(finishedTaskCount, top);
            response.finishedTaskCount = finishedTaskCount;
        }
        response.topNumber = top;
        return response;
    }

    private MRTaskExecutionResponse.TaskGroupResponse getTaskGroups(@QueryParam("site") String site,
                                                                   @QueryParam("shortJob_id") String shortDurationJobId,
                                                                   @QueryParam("longJob_id") String longDurationJobId) {
        MRTaskExecutionResponse.TaskGroupResponse result = new MRTaskExecutionResponse.TaskGroupResponse();
        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, shortDurationJobId);
        GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> smallResponse = ResourceUtils.getQueryResult(query, null, null);
        if (!smallResponse.isSuccess() || smallResponse.getObj() == null) {
            result.errMessage = smallResponse.getException();
            return result;
        }
        long longestDuration = 0;
        for (TaskExecutionAPIEntity entity : smallResponse.getObj()) {
            if (entity.getDuration() > longestDuration) {
                longestDuration = entity.getDuration();
            }
        }
        query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, longDurationJobId);
        GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> largeResponse = ResourceUtils.getQueryResult(query, null, null);
        if (!largeResponse.isSuccess() || largeResponse.getObj() == null) {
            result.errMessage = largeResponse.getException();
            return result;
        }
        result.tasksGroupByType = new HashMap<>();
        result.tasksGroupByType.put(Constants.TaskType.MAP.toString(), new MRTaskExecutionResponse.TaskGroup());
        result.tasksGroupByType.put(Constants.TaskType.REDUCE.toString(), new MRTaskExecutionResponse.TaskGroup());
        groupTasksByValue(result, false, largeResponse.getObj(), longestDuration);
        groupTasksByValue(result, true, smallResponse.getObj(), longestDuration);

        return result;
    }

    public MRTaskExecutionResponse.TaskGroupResponse groupTasksByValue(MRTaskExecutionResponse.TaskGroupResponse result, boolean keepShort, List<TaskExecutionAPIEntity> tasks, long value) {
        for (TaskExecutionAPIEntity entity : tasks) {
            String taskType = entity.getTags().get(MRJobTagName.TASK_TYPE.toString());
            MRTaskExecutionResponse.TaskGroup taskGroup = result.tasksGroupByType.get(taskType.toUpperCase());
            if (entity.getDuration() <= value && keepShort) {
                taskGroup.shortTasks.add(entity);
            }
            if (entity.getDuration() > value) {
                taskGroup.longTasks.add(entity);
            }
        }
        return result;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("taskSuggestion")
    public List<MRTaskExecutionResponse.JobSuggestionResponse> getSuggestion(@QueryParam("site") String site,
                                                                             @QueryParam("shortJob_id") String shortDurationJobId,
                                                                             @QueryParam("longJob_id") String longDurationJobId,
                                                                             @QueryParam("thresholdValue") long threshold) {
        List<MRTaskExecutionResponse.JobSuggestionResponse> result = new ArrayList<>();
        MRTaskExecutionResponse.TaskGroupResponse taskGroups = getTaskGroups(site, shortDurationJobId, longDurationJobId);
        if (taskGroups.errMessage != null) {
            LOG.error(taskGroups.errMessage);
            return result;
        }
        List<SuggestionFunc> suggestionFuncs = new ArrayList<>();
        suggestionFuncs.add(new MapInputFunc(threshold));
        suggestionFuncs.add(new ReduceInputFunc(threshold));
        suggestionFuncs.add(new MapGCFunc(threshold));
        suggestionFuncs.add(new ReduceGCFunc(threshold));
        suggestionFuncs.add(new MapSpillFunc(threshold));
        try {
            for (SuggestionFunc func : suggestionFuncs) {
                result.add(func.apply(taskGroups));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return result;
        }
        return  result;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("historyTaskCount")
    public MRJobTaskCountResponse.HistoryTaskCountResponse getTaskCountInMinute(@QueryParam("site") String site,
                                                                                   @QueryParam("jobId") String jobId,
                                                                                   @QueryParam("jobStartTime") String jobStartTime,
                                                                                   @QueryParam("jobEndTime") String jobEndTime) {
        MRJobTaskCountResponse.HistoryTaskCountResponse result = new MRJobTaskCountResponse.HistoryTaskCountResponse();
        if (jobId == null || site == null || jobStartTime == null || jobEndTime == null) {
            result.errMessage = "IllegalArgumentException: jobId, or site, or jobStartTime, or jobEndTime is null";
            return result;
        }

        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{@startTime,@endTime,@taskType}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, jobId);
        GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> response = ResourceUtils.getQueryResult(query, jobStartTime, jobEndTime);
        if (!response.isSuccess() || response.getObj() == null) {
            result.errMessage = response.getException();
            return result;
        }
        try {
            long startTimeInMin = DateTimeUtil.humanDateToSeconds(jobStartTime) / 60;
            long endTimeInMin = DateTimeUtil.humanDateToSeconds(jobEndTime) / 60;
            return taskCountImpl.countHistoryTask(response.getObj(), startTimeInMin, endTimeInMin);
        } catch (Exception e) {
            e.printStackTrace();
            result.errMessage = e.getMessage();
            return result;
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("taskDistribution/{counterName}")
    public MRTaskExecutionResponse.TaskDistributionResponse getTaskDistributionByCounterName(@QueryParam("site") String site,
                                                                              @QueryParam("jobId") String jobId,
                                                                              @QueryParam("jobStartTime") String jobStartTime,
                                                                              @QueryParam("jobEndTime") String jobEndTime,
                                                                              @QueryParam("taskType") String taskType,
                                                                              @PathParam("counterName") String counterName,
                                                                              @QueryParam("distRange") String distRange) {
        MRTaskExecutionResponse.TaskDistributionResponse result = new MRTaskExecutionResponse.TaskDistributionResponse();
        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\" AND @taskType=\"%s\"]{@jobCounters}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME,
            site, jobId, Constants.TaskType.MAP.toString());
        GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> response = ResourceUtils.getQueryResult(query, jobStartTime, jobEndTime);
        if (!response.isSuccess() || response.getObj() == null) {
            result.errMessage = response.getException();
            return result;
        }
        try {
            return taskCountImpl.getHistoryTaskDistribution(response.getObj(), counterName, distRange);
        } catch (Exception e) {
            e.printStackTrace();
            result.errMessage = e.getMessage();
            return result;
        }
    }


}
