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

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.JobCountResponse;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.service.jpm.count.MRJobCountImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("mrJobs")
public class MRJobExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(MRJobExecutionResource.class);

    public static final String ELAPSEDMS = "elapsedms";
    public static final String TOTAL_RESULTS = "totalResults";

    private final MRJobCountImpl helper = new MRJobCountImpl();
    private GenericEntityServiceResource resource = new GenericEntityServiceResource();

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
                                                    @QueryParam("verbose") Boolean verbose) throws ParseException {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();

        List<TaggedLogAPIEntity> jobs = new ArrayList<>();
        List<JobExecutionAPIEntity> finishedJobs = new ArrayList<>();
        Set<String> jobIds = new HashSet<>();
        final Map<String, Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        String jobQuery = String.format(query, Constants.MR_JOB_EXECUTION_SERVICE_NAME);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> res =
            resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing, parallel, metricName, verbose);
        if (res.isSuccess() && res.getObj() != null) {
            long maxFinishedTime = DateTimeUtil.humanDateToSeconds(endTime) * DateTimeUtil.ONESECOND;
            for (JobExecutionAPIEntity o : res.getObj()) {
                if (o.getEndTime() <= maxFinishedTime) {
                    finishedJobs.add(o);
                    jobIds.add(o.getTags().get(MRJobTagName.JOB_ID.toString()));
                }
            }
            jobQuery = String.format(query, Constants.MR_RUNNING_JOB_EXECUTION_SERVICE_NAME);
            GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity> runningRes =
                    resource.search(jobQuery, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing, parallel, metricName, verbose);
            if (runningRes.isSuccess() && runningRes.getObj() != null) {
                for (org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity o : runningRes.getObj()) {
                    String key = o.getTags().get(MRJobTagName.JOB_ID.toString());
                    if (!ResourceUtils.isDuplicate(jobIds, key)) {
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
        String queryString = String.format(queryFormat, Constants.MR_JOB_EXECUTION_SERVICE_NAME, condition);
        GenericServiceAPIResponseEntity<TaggedLogAPIEntity> res = ResourceUtils.getQueryResult(queryString, null, null);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                jobs.add(o);
                jobIds.add(o.getTags().get(MRJobTagName.JOB_ID.toString()));
            }
        }
        queryString = String.format(queryFormat, Constants.MR_RUNNING_JOB_EXECUTION_SERVICE_NAME, condition);
        res = ResourceUtils.getQueryResult(queryString, null, null);
        if (res.isSuccess() && res.getObj() != null) {
            for (TaggedLogAPIEntity o : res.getObj()) {
                String key = o.getTags().get(MRJobTagName.JOB_ID.toString());
                if (!ResourceUtils.isDuplicate(jobIds, key)) {
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
    @Path("runningJobCounts")
    @Produces(MediaType.APPLICATION_JSON)
    public JobCountResponse getRunningJobCount(@QueryParam("site") String site,
                                               @QueryParam("durationBegin") String startTime,
                                               @QueryParam("durationEnd") String endTime,
                                               @QueryParam("intervalInSecs") long intervalInSecs) {
        JobCountResponse response = new JobCountResponse();
        if (site == null || startTime == null || endTime == null) {
            response.errMessage = "IllegalArgument: site, durationBegin, durationEnd is null";
            return response;
        }
        if (intervalInSecs <= 0) {
            response.errMessage = String.format("IllegalArgument: intervalInSecs=%s is invalid", intervalInSecs);
            return response;
        }
        String searchStartTime = startTime;
        String searchEndTime = endTime;
        try {
            searchStartTime = helper.moveTimeForwardOneDay(searchStartTime);
        } catch (Exception e) {
            response.errMessage = e.getMessage();
            return response;
        }
        String query = String.format("%s[@site=\"%s\"]{@startTime,@endTime,@jobType,@jobId}", Constants.MR_JOB_EXECUTION_SERVICE_NAME, site);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes = ResourceUtils.getQueryResult(query, searchStartTime, searchEndTime);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            response.errMessage = String.format("Catch an exception during fetch history jobs: %s with query=%s", historyRes.getException(), query);
            return response;
        }
        query = String.format("%s[@site=\"%s\"]{@startTime,@endTime,@jobType,@jobId}", Constants.MR_RUNNING_JOB_EXECUTION_SERVICE_NAME, site);
        GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity> runningRes = ResourceUtils.getQueryResult(query, searchStartTime, searchEndTime);
        if (!runningRes.isSuccess() || runningRes.getObj() == null) {
            response.errMessage = String.format("Catch an exception during fetch running jobs: %s with query=%s", runningRes.getException(), query);
            return response;
        }

        try {
            long startTimeInSecs = DateTimeUtil.humanDateToSeconds(startTime);
            long endTimeInSecs = DateTimeUtil.humanDateToSeconds(endTime);
            return helper.getRunningJobCount(historyRes.getObj(), runningRes.getObj(), startTimeInSecs, endTimeInSecs, intervalInSecs);
        } catch (Exception e) {
            e.printStackTrace();
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
            searchStartTime = helper.moveTimeForwardOneDay(searchStartTime);
        } catch (ParseException e) {
            response.setException(e);
            response.setSuccess(false);
            return response;
        }
        String query = String.format("%s[@site=\"%s\" AND @startTime<=\"%s\" AND @endTime>=\"%s\"]{@startTime,@endTime}",
            Constants.MR_JOB_EXECUTION_SERVICE_NAME, site, timePointsInMills, timePointsInMills);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes = ResourceUtils.getQueryResult(query, searchStartTime, searchEndTime);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            return historyRes;
        }

        List<String> timeDuration = helper.getSearchTimeDuration(historyRes.getObj());
        LOG.info(String.format("new search time range: startTime=%s, endTime=%s", timeDuration.get(0), timeDuration.get(1)));
        query = String.format("%s[@site=\"%s\"]<@jobId>{sum(value)}.{sum(value) desc}", Constants.GENERIC_METRIC_SERVICE, site);
        return metricQueryFunc.apply(query, timeDuration.get(0), timeDuration.get(1), intervalmin, top, metricName);
    }

    private Function6<String, String, String, Long, Integer, String, Object> queryMetricEntitiesFunc
        = (query, startTime, endTime, intervalmin, top, metricName) -> {
            GenericEntityServiceResource resource = new GenericEntityServiceResource();
            return resource.search(query, startTime, endTime, Integer.MAX_VALUE, null,
            false, true, intervalmin, top, true, 0, metricName, false);
        };

    private Function6<String, String, String, Long, Integer, String, Object> queryMetricListFunc
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
    @Path("jobCountsByDuration")
    @Produces(MediaType.APPLICATION_JSON)
    public JobCountResponse getJobCountGroupByDuration(@QueryParam("site") String site,
                                                       @QueryParam("timeDistInSecs") String timeList,
                                                       @QueryParam("startTime") String startTime,
                                                       @QueryParam("endTime") String endTime,
                                                       @QueryParam("jobType") String jobType) {
        JobCountResponse response = new JobCountResponse();
        if (site == null || startTime == null || endTime == null || timeList == null) {
            response.errMessage = "IllegalArgument: site, startTime, endTime, or timeDistInSecs is null";
            return response;
        }
        String query = String.format("%s[@site=\"%s\"]{@durationTime,@jobType}", Constants.MR_JOB_EXECUTION_SERVICE_NAME, site);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> historyRes = ResourceUtils.getQueryResult(query, startTime, endTime);
        if (!historyRes.isSuccess() || historyRes.getObj() == null) {
            response.errMessage = String.format("Catch an exception: %s with query=%s", historyRes.getException(), query);
            return response;
        }
        try {
            if (jobType != null) {
                List<JobExecutionAPIEntity> jobs = new ArrayList<>();
                for (JobExecutionAPIEntity o : historyRes.getObj()) {
                    if (o.getTags().get(MRJobTagName.JOB_TYPE.toString()).equalsIgnoreCase(jobType)) {
                        jobs.add(o);
                    }
                }
                return helper.getHistoryJobCountGroupByDuration(jobs, timeList);
            } else {
                return helper.getHistoryJobCountGroupByDuration(historyRes.getObj(), timeList);
            }
        } catch (Exception e) {
            response.errMessage = e.getMessage();
            return response;
        }

    }
}
