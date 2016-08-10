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
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
        int pageSize = 10000;
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

}
