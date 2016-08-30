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

import static org.apache.eagle.jpm.util.MRJobTagName.JOB_ID;
import static org.apache.eagle.jpm.util.MRJobTagName.TASK_TYPE;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.JobCountResponse;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.TaskCountPerJobResponse;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("mrJobs")
public class MRJobExecutionResource {
    GenericEntityServiceResource resource = new GenericEntityServiceResource();
    public static final String ELAPSEDMS = "elapsedms";
    public static final String TOTAL_RESULTS = "totalResults";

    private static final Logger LOG = LoggerFactory.getLogger(MRJobExecutionResource.class);

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
        final Map<String, Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        String jobQuery = String.format(query, Constants.JPA_JOB_EXECUTION_SERVICE_NAME);
        GenericServiceAPIResponseEntity<TaggedLogAPIEntity> res =
            resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin,
                top, filterIfMissing, parallel, metricName, verbose);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                finishedJobs.add(o);
                jobIds.add(o.getTags().get(JOB_ID.toString()));
            }
            jobQuery = String.format(query, Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME);
            res = resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin,
                top, filterIfMissing, parallel, metricName, verbose);
            if (res.isSuccess() && res.getObj() != null) {
                for (TaggedLogAPIEntity o : res.getObj()) {
                    if (!isDuplicate(jobIds, o)) {
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
        meta.put(ELAPSEDMS, stopWatch.getTime());
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
        String conditionFormat = "@site=\"%s\"";
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
        if ((jobId == null && jobDefId == null) || site == null) {
            response.setException(new IllegalArgumentException("Error: (jobId == null && jobDefId == null) || site == null"));
            response.setSuccess(false);
            return response;
        }

        List<TaggedLogAPIEntity> jobs = new ArrayList<>();
        Set<String> jobIds = new HashSet<>();
        String condition = buildCondition(jobId, jobDefId, site);
        final int pageSize = Integer.MAX_VALUE;
        if (condition == null) {
            response.setException(new Exception("Search condition is empty"));
            response.setSuccess(false);
            return response;
        }
        LOG.debug("search condition=" + condition);

        final Map<String, Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        String queryFormat = "%s[%s]{*}";
        String queryString = String.format(queryFormat, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, condition);
        GenericServiceAPIResponseEntity<TaggedLogAPIEntity> res = resource.search(queryString, null, null, pageSize, null, false, true, 0L, 0, true, 0, null, false);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                jobs.add(o);
                jobIds.add(o.getTags().get(JOB_ID.toString()));
            }
        }
        queryString = String.format(queryFormat, Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME, condition);
        res = resource.search(queryString, null, null, pageSize, null, false, true, 0L, 0, true, 0, null, false);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                if (!isDuplicate(jobIds, o)) {
                    jobs.add(o);
                }
            }
        }
        if (jobs.size() > 0) {
            Collections.sort(jobs, new Comparator<TaggedLogAPIEntity>() {
                @Override
                public int compare(TaggedLogAPIEntity o1, TaggedLogAPIEntity o2) {
                    return o1.getTimestamp() > o2.getTimestamp() ? 1 : (o1.getTimestamp() == o2.getTimestamp() ? 0 : -1);
                }
            });
        }
        stopWatch.stop();
        if (res.isSuccess()) {
            response.setSuccess(true);
        } else {
            response.setSuccess(false);
            response.setException(new Exception(res.getException()));
        }
        meta.put(TOTAL_RESULTS, jobs.size());
        meta.put(ELAPSEDMS, stopWatch.getTime());
        response.setObj(jobs);
        response.setMeta(meta);
        return response;
    }




    @GET
    @Path("{jobId}/taskCounts")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskCountPerJobResponse getTaskCountsPerJob(@PathParam("jobId") String jobId,
                                                       @QueryParam("site") String site,
                                                       @QueryParam("timelineInSecs") String timeList,
                                                       @QueryParam("top") long top) {
        TaskCountPerJobResponse response = new TaskCountPerJobResponse();
        if (jobId == null || site == null || timeList == null || timeList.isEmpty()) {
            response.errMessage = "IllegalArgumentException: jobId == null || site == null || timelineInSecs == null or isEmpty";
            return response;
        }
        TaskCountByDurationHelper helper = new TaskCountByDurationHelper();
        List<MRJobTaskCountResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        List<MRJobTaskCountResponse.UnitTaskCount> finishedTaskCount = new ArrayList<>();

        List<Long> times = helper.parseTimeList(timeList);
        String query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_TASK_EXECUTION_SERVICE_NAME, site, jobId);
        GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> historyRes =
            resource.search(query, null, null, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
        if (historyRes.isSuccess() && historyRes.getObj() != null && historyRes.getObj().size() > 0) {
            helper.initTaskCountList(runningTaskCount, finishedTaskCount, times, new TaskCountByDurationHelper.HistoryTaskComparator());
            for (org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o : historyRes.getObj()) {
                int index = helper.getPosition(times, o.getDuration());
                MRJobTaskCountResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                helper.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                counter.entities.add(o);
            }
        } else {
            query = String.format("%s[@site=\"%s\" AND @jobId=\"%s\"]{*}", Constants.JPA_RUNNING_TASK_EXECUTION_SERVICE_NAME, site, jobId);
            GenericServiceAPIResponseEntity<TaskExecutionAPIEntity> runningRes =
                resource.search(query, null, null, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
            if (runningRes.isSuccess() && runningRes.getObj() != null) {
                helper.initTaskCountList(runningTaskCount, finishedTaskCount, times, new TaskCountByDurationHelper.RunningTaskComparator());
                for (TaskExecutionAPIEntity o : runningRes.getObj()) {
                    int index = helper.getPosition(times, o.getDuration());
                    if (o.getTaskStatus().equalsIgnoreCase(Constants.TaskState.RUNNING.toString())) {
                        MRJobTaskCountResponse.UnitTaskCount counter = runningTaskCount.get(index);
                        helper.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                        counter.entities.add(o);
                    } else if (o.getEndTime() != 0) {
                        MRJobTaskCountResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                        helper.countTask(counter, o.getTags().get(TASK_TYPE.toString()));
                        counter.entities.add(o);
                    }
                }
            }
        }
        if (top > 0) {
            helper.getTopTasks(runningTaskCount, top);
            response.runningTaskCount = runningTaskCount;
            helper.getTopTasks(finishedTaskCount, top);
            response.finishedTaskCount = finishedTaskCount;
        }
        response.topNumber = top;
        return response;
    }

    @GET
    @Path("runningJobCounts")
    @Produces(MediaType.APPLICATION_JSON)
    public JobCountResponse getRunningJobCount(@QueryParam("site") String site,
                                               @QueryParam("durationBegin") String startTime,
                                               @QueryParam("durationEnd") String endTime,
                                               @QueryParam("intervalInSecs") long intervalInSecs) {
        JobCountResponse response = new JobCountResponse();
        MRJobCountHelper helper = new MRJobCountHelper();
        if (site == null || startTime == null || endTime == null) {
            response.errMessage = "IllegalArgument: site, durationBegin, durationEnd, or metric is null";
            return response;
        }
        if (intervalInSecs <= 0) {
            response.errMessage = String.format("IllegalArgument: intervalInSecs=%s is invalid", intervalInSecs);
            return response;
        }
        long startTimeInMills;
        String searchStartTime = startTime;
        String searchEndTime = endTime;
        try {
            startTimeInMills = DateTimeUtil.humanDateToSeconds(startTime) * DateTimeUtil.ONESECOND;
            searchStartTime = helper.moveTimeforwardOneDay(searchStartTime);
        } catch (Exception e) {
            response.errMessage = e.getMessage();
            return response;
        }
        String query = String.format("%s[@site=\"%s\" AND @endTime>=%s]{@startTime,@endTime,@jobType}", Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, startTimeInMills);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes =
            resource.search(query, searchStartTime, searchEndTime, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            response.errMessage = String.format("Catch an exception: %s with query=%s", historyRes.getException(), query);
            return response;
        }

        try {
            long startTimeInSecs = DateTimeUtil.humanDateToSeconds(startTime);
            long endTimeInSecs = DateTimeUtil.humanDateToSeconds(endTime);
            return helper.getRunningJobCount(historyRes.getObj(), startTimeInSecs, endTimeInSecs, intervalInSecs);
        } catch (Exception e) {
            response.errMessage = e.getMessage();
            return response;
        }
    }

    @GET
    @Path("jobMetrics/entities")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getJobMetricsByEntitiesQuery(@QueryParam("site") String site,
                                               @QueryParam("timePoint") String timePoint,
                                               @QueryParam("metricName") String metricName,
                                               @QueryParam("intervalmin") long intervalmin,
                                               @QueryParam("top") int top) {
        return getJobMetrics(site, timePoint, metricName, intervalmin, top, queryMetricEntitiesFunc);
    }

    @GET
    @Path("jobMetrics/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getJobMetricsByListQuery(@QueryParam("site") String site,
                                           @QueryParam("timePoint") String timePoint,
                                           @QueryParam("metricName") String metricName,
                                           @QueryParam("intervalmin") long intervalmin,
                                           @QueryParam("top") int top) {
        return getJobMetrics(site, timePoint, metricName, intervalmin, top, queryMetricListFunc);
    }

    public Object getJobMetrics(String site, String timePoint, String metricName, long intervalmin, int top,
                                Function6<String, String, String, Long, Integer, String, Object> metricQueryFunc) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        MRJobCountHelper helper = new MRJobCountHelper();
        if (site == null || timePoint == null || metricName == null) {
            response.setException(new IllegalArgumentException("Error: site, timePoint, metricName may be unset"));
            response.setSuccess(false);
            return response;
        }
        if (intervalmin <= 0) {
            LOG.warn("query parameter intervalmin <= 0, use default value 5 instead");
            intervalmin = 5;
        }
        if (top <= 0) {
            LOG.warn("query parameter top <= 0, use default value 10 instead");
            top = 10;
        }

        long timePointsInMills;
        String searchStartTime = timePoint;
        String searchEndTime = timePoint;
        try {
            timePointsInMills = DateTimeUtil.humanDateToSeconds(timePoint) * DateTimeUtil.ONESECOND;
            searchStartTime = helper.moveTimeforwardOneDay(searchStartTime);
        } catch (ParseException e) {
            response.setException(e);
            response.setSuccess(false);
            return response;
        }
        String query = String.format("%s[@site=\"%s\" AND @startTime<=\"%s\" AND @endTime>=\"%s\"]{@startTime,@endTime}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, timePointsInMills, timePointsInMills);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes =
            resource.search(query, searchStartTime, searchEndTime, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            return historyRes;
        }

        List<String> timeDuration = helper.getSearchTimeDuration(historyRes.getObj());
        LOG.info(String.format("new search time range: startTime=%s, endTime=%s", timeDuration.get(0), timeDuration.get(1)));
        query = String.format("%s[@site=\"%s\"]<@jobId>{sum(value)}.{sum(value) desc}", Constants.GENERIC_METRIC_SERVICE, site);
        return metricQueryFunc.apply(query, timeDuration.get(0), timeDuration.get(1), intervalmin, top, metricName);
    }

    Function6<String, String, String, Long, Integer, String, Object> queryMetricEntitiesFunc
        = (query, startTime, endTime, intervalmin, top, metricName) -> {
            GenericEntityServiceResource resource = new GenericEntityServiceResource();
            return resource.search(query, startTime, endTime, Integer.MAX_VALUE, null,
            false, true, intervalmin, top, true, 0, metricName, false);
        };

    Function6<String, String, String, Long, Integer, String, Object> queryMetricListFunc
        = (query, startTime, endTime, intervalmin, top, metricName) -> {
            ListQueryResource resource = new ListQueryResource();
            return resource.listQuery(query, startTime, endTime, Integer.MAX_VALUE, null,
            false, true, intervalmin, top, true, 0, metricName, false);
        };

    @FunctionalInterface
    interface Function6<A, B, C, D, E, F, R> {
        R apply(A a, B b, C c, D d, E e, F f);
    }

    @GET
    @Path("jobCount")
    @Produces(MediaType.APPLICATION_JSON)
    public JobCountResponse getJobCountGroupByDuration(@QueryParam("site") String site,
                                                       @QueryParam("timelineInSecs") String timeList,
                                                       @QueryParam("jobStartTimeBegin") String startTime,
                                                       @QueryParam("jobStartTimeEnd") String endTime) {
        JobCountResponse response = new JobCountResponse();
        MRJobCountHelper helper = new MRJobCountHelper();
        if (site == null || startTime == null || endTime == null || timeList == null) {
            response.errMessage = "IllegalArgument: site, jobStartTimeBegin, jobStartTimeEnd, or timelineInSecs is null";
            return response;
        }
        String query = String.format("%s[@site=\"%s\"]{@durationTime,@jobType}", Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes =
            resource.search(query, startTime, endTime, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            response.errMessage = String.format("Catch an exception: %s with query=%s", historyRes.getException(), query);
            return response;
        }
        try {
            return helper.getHistoryJobCount(historyRes.getObj(), timeList);
        } catch (Exception e) {
            response.errMessage = e.getMessage();
            return response;
        }

    }
}
