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

package org.apache.eagle.service.jpm;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static org.apache.eagle.jpm.util.MRJobTagName.JOB_ID;

@Path("mrJobs")
public class MRJobExecutionResource {
    GenericEntityServiceResource resource = new GenericEntityServiceResource();
    public final static String ELAPSEDMS = "elapsedms";
    public final static String TOTAL_RESULTS = "totalResults";

    private final static Logger LOG = LoggerFactory.getLogger(MRJobExecutionResource.class);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity listJobs(@QueryParam("query") String query,
                                                    @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
                                                    @QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
                                                    @QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
                                                    @QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
                                                    @QueryParam("filterIfMissing") boolean filterIfMissing,
                                                    @QueryParam("parallel") int parallel,
                                                    @QueryParam("metricName") String metricName,
                                                    @QueryParam("verbose") Boolean verbose) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();

        List<TaggedLogAPIEntity> jobs = new ArrayList<>();
        List<TaggedLogAPIEntity> finishedJobs = new ArrayList<>();
        Set<String> jobIds = new HashSet<>();
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        String jobQuery = String.format(query, Constants.JPA_JOB_EXECUTION_SERVICE_NAME);
        GenericServiceAPIResponseEntity<TaggedLogAPIEntity> res =
                resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin,
                top,filterIfMissing, parallel, metricName, verbose);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                finishedJobs.add(o);
                jobIds.add(o.getTags().get(JOB_ID.toString()));
            }
            jobQuery = String.format(query, Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME);
            res = resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin,
                    top,filterIfMissing, parallel, metricName, verbose);
            if (res.isSuccess() && res.getObj() != null) {
                for (TaggedLogAPIEntity o : res.getObj()) {
                    if (! isDuplicate(jobIds, o)) {
                        jobs.add(o);
                    }
                }
                jobs.addAll(finishedJobs);
            }
        }
        stopWatch.stop();
        if (res.isSuccess()) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
            response.setException(new Exception(res.getException()));
        }
        meta.put(TOTAL_RESULTS, jobs.size());
        meta.put(ELAPSEDMS,stopWatch.getTime());
        response.setObj(jobs);
        response.setMeta(meta);
        return response;

    }

    private boolean isDuplicate(Set<String> keys, TaggedLogAPIEntity o) {
        if (keys.isEmpty()) {
            return false;
        }
        return keys.contains(o.getTags().get(JOB_ID.toString()));
    }

    private String buildCondition(String jobId, String jobDefId, String site) {
        String conditionFormat = "@site=\"%s\"" ;
        String condition = null;
        if (jobDefId != null) {
            conditionFormat = conditionFormat + " AND @jobDefId=\"%s\"";
            condition = String.format(conditionFormat, site, jobDefId);
        }
        if (jobId != null) {
            conditionFormat = conditionFormat + " AND @jobId=\"%s\"";
            condition = String.format(conditionFormat, site, jobId);
        }
        return condition;
    }

    @GET
    @Path("search")
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity searchJobsById(@QueryParam("jobId") String jobId,
                                                          @QueryParam("jobDefId") String jobDefId,
                                                          @QueryParam("site") String site) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        List<TaggedLogAPIEntity> jobs = new ArrayList<>();
        String condition = buildCondition(jobId, jobDefId, site);
        int pageSize = Integer.MAX_VALUE;
        if (condition == null) {
            response.setException(new Exception("Search condition is empty"));
            response.setSuccess(false);
            return response;
        }
        LOG.debug("search condition=" + condition);

        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        String queryFormat = "%s[%s]{*}";
        String queryString = String.format(queryFormat, Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME, condition);
        GenericServiceAPIResponseEntity<TaggedLogAPIEntity> res =
                resource.search(queryString, null, null, pageSize, null, false, true,  0L, 0, true, 0, null, false);
        if (res.isSuccess() && res.getObj() != null) {
            jobs.addAll(res.getObj());
        }
        queryString = String.format(queryFormat, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, condition);
        res = resource.search(queryString, null, null, pageSize, null, false, true,  0L, 0, true, 0, null, false);
        if (res.isSuccess() && res.getObj() != null) {
            jobs.addAll(res.getObj());
        }
        stopWatch.stop();
        if (res.isSuccess()) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
            response.setException(new Exception(res.getException()));
        }
        meta.put(TOTAL_RESULTS, jobs.size());
        meta.put(ELAPSEDMS,stopWatch.getTime());
        response.setObj(jobs);
        response.setMeta(meta);
        return response;
    }

    public List<Long> parseTimeList(String timelist) {
        List<Long> times = new ArrayList<>();
        String [] strs = timelist.split("[,\\s]");
        for (String str : strs) {
            try {
                times.add(Long.parseLong(str));
            } catch (Exception ex) {
                LOG.warn(str + " is not a number");
            }
        }
        return times;
    }

    public int getPosition(List<Long> times, Long duration) {
        duration = duration / 1000;
        for (int i = 1; i < times.size(); i++) {
            if (duration < times.get(i)) {
                return i - 1;
            }
        }
        return times.size() - 1;
    }

    public void getTopTasks(List<MRJobTaskGroupResponse.UnitTaskCount> list, long top) {
        for (MRJobTaskGroupResponse.UnitTaskCount taskCounter : list) {
            Iterator<TaskExecutionAPIEntity> iterator = taskCounter.entities.iterator();
            for (int i = 0; i < top && iterator.hasNext(); i++) {
                taskCounter.topEntities.add(iterator.next());
            }
            taskCounter.entities.clear();
        }
    }

    public void initTaskCountList(List<MRJobTaskGroupResponse.UnitTaskCount> runningTaskCount,
                                  List<MRJobTaskGroupResponse.UnitTaskCount> finishedTaskCount,
                                  List<Long> times,
                                  Comparator comparator) {
        for (int i = 0; i < times.size(); i++) {
            runningTaskCount.add(new MRJobTaskGroupResponse.UnitTaskCount(times.get(i), comparator));
            finishedTaskCount.add(new MRJobTaskGroupResponse.UnitTaskCount(times.get(i), comparator));
        }
    }

    @GET
    @Path("{jobId}/taskCounts")
    @Produces(MediaType.APPLICATION_JSON)
    public MRJobTaskGroupResponse getTaskCounts(@PathParam("jobId") String jobId,
                                                @QueryParam("site") String site,
                                                @QueryParam("times") String timeList,
                                                @QueryParam("top") long top) {
        MRJobTaskGroupResponse response = new MRJobTaskGroupResponse();
        List<Long> times = parseTimeList(timeList);

        if (jobId == null || site == null || timeList.isEmpty()) {
            response.errMessage = "Error: jobId == null || site == null || timeList.isEmpty()";
            return response;
        }
        List<MRJobTaskGroupResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        List<MRJobTaskGroupResponse.UnitTaskCount> finishedTaskCount = new ArrayList<>();

        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, jobId);
        GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> history_res =
                resource.search(query,  null, null, Integer.MAX_VALUE, null, false, true,  0L, 0, true, 0, null, false);
        if (history_res.isSuccess() && history_res.getObj() != null && history_res.getObj().size() > 0) {
            initTaskCountList(runningTaskCount, finishedTaskCount, times, new HistoryTaskComparator());
            for (org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o : history_res.getObj()) {
                int index = getPosition(times, o.getDuration());
                MRJobTaskGroupResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                counter.taskCount++;
                counter.entities.add(o);
            }
        } else {
            query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_RUNNING_TASK_EXECUTION_SERVICE_NAME, site, jobId);
            GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> running_res =
                    resource.search(query,  null, null, Integer.MAX_VALUE, null, false, true,  0L, 0, true, 0, null, false);
            if (running_res.isSuccess() && running_res.getObj() != null) {
                initTaskCountList(runningTaskCount, finishedTaskCount, times, new RunningTaskComparator());
                for (TaskExecutionAPIEntity o : running_res.getObj()) {
                    int index = getPosition(times, o.getDuration());
                    if (o.getTaskStatus().equalsIgnoreCase(Constants.TaskState.RUNNING.toString())) {
                        MRJobTaskGroupResponse.UnitTaskCount counter = runningTaskCount.get(index);
                        counter.taskCount++;
                        counter.entities.add(o);
                    } else if (o.getEndTime() != 0) {
                        MRJobTaskGroupResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                        counter.taskCount++;
                        counter.entities.add(o);
                    }
                }
            }
        }
        if (top > 0)  {
            getTopTasks(runningTaskCount, top);
            response.runningTaskCount = runningTaskCount;
            getTopTasks(finishedTaskCount, top);
            response.finishedTaskCount = finishedTaskCount;
        }
        return response;
    }

    static class RunningTaskComparator implements Comparator<TaskExecutionAPIEntity> {
        @Override
        public int compare(TaskExecutionAPIEntity o1, TaskExecutionAPIEntity o2) {
            Long time1 = o1.getDuration();
            Long time2 = o2.getDuration();
            return (time1 > time2 ? -1 : (time1 == time2) ? 0 : 1);
        }
    }

    static class HistoryTaskComparator implements Comparator<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> {
        @Override
        public int compare(org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o1,
                           org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o2) {
            Long time1 = o1.getDuration();
            Long time2 = o2.getDuration();
            return (time1 > time2 ? -1 : (time1 == time2) ? 0 : 1);
        }
    }

}
