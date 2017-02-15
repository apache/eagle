/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
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
import org.apache.eagle.common.utils.Tuple2;
import org.apache.eagle.hadoop.queue.model.scheduler.QueueStructureAPIEntity;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.QUEUE_MAPPING_SERVICE_NAME;
import static org.apache.eagle.jpm.util.Constants.JPA_JOB_EXECUTION_SERVICE_NAME;
import static org.apache.eagle.jpm.util.Constants.JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME;
import static org.apache.eagle.jpm.util.MRJobTagName.JOB_ID;
import static org.apache.eagle.jpm.util.MRJobTagName.JOB_QUEUE;
import static org.apache.eagle.jpm.util.MRJobTagName.USER;

@Path("queue")
public class RunningQueueResource {

    @GET
    @Path("memory")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RunningQueueResponse getTopByQueue(@QueryParam("site") String site,
                                              @QueryParam("queue") String queue,
                                              @QueryParam("currentTime") long currentTime,
                                              @QueryParam("top") int top) {
        RunningQueueResponse result = new RunningQueueResponse();
        try {
            if (site == null || queue == null || currentTime == 0L || top == 0) {
                throw new Exception("Invalid query parameters: site == null || queue == null || currentTime == 0L || top == 0");
            }
            Tuple2<String, String> queryTimeRange = getQueryTimeRange(currentTime);
            Map<String, Set<String>> queueMap = getQueueMap(site);
            List<JobExecutionAPIEntity> runningJobs = getRunningJobs(site, currentTime, queryTimeRange.f0(), queryTimeRange.f1());
            List<org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity> jobs = getJobs(site, currentTime, queryTimeRange.f0(), queryTimeRange.f1());
            Set<String> jobIds = new HashSet<>();
            jobs.forEach(job -> jobIds.add(job.getTags().get(JOB_ID.toString())));

            Map<String, Long> userUsage = new HashMap<>();
            Map<String, Long> jobUsage = new HashMap<>();
            for (JobExecutionAPIEntity job : runningJobs) {
                String jobId = job.getTags().get(JOB_ID.toString());
                String jobQueue = job.getTags().get(JOB_QUEUE.toString());
                String user = job.getTags().get(USER.toString());

                if (jobIds.contains(jobId) && queueMap.containsKey(queue)
                        && (queueMap.containsKey(jobQueue) || queueMap.get(queue).contains(jobQueue))) {
                    if (userUsage.containsKey(user)) {
                        userUsage.put(user, userUsage.get(user) + job.getAllocatedMB());
                    } else {
                        userUsage.put(user, 0L);
                    }
                    jobUsage.put(jobId, job.getAllocatedMB());
                }
            }
            result.setJobs(getTopRecords(top, jobUsage));
            result.setUsers(getTopRecords(top, userUsage));
         } catch (Exception e) {
            result.setErrMessage(e.getMessage());
        }
        return result;
    }

    private List<JobExecutionAPIEntity> getRunningJobs(String site, long currentTime, String startTime, String endTime) throws Exception {
        GenericEntityServiceResource resource = new GenericEntityServiceResource();
        String query = String.format("%s[@site=\"%s\" and @startTime<=%s and (@internalState=\"RUNNING\" or @endTime>%s)]{@jobId, @user, @queue, @allocatedMB}", JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME, site, currentTime, currentTime);
        GenericServiceAPIResponseEntity<JobExecutionAPIEntity> runningJobResponse = resource.search(query, startTime, endTime, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);

        if (!runningJobResponse.isSuccess() || runningJobResponse.getObj() == null) {
            throw new IOException(runningJobResponse.getException());
        }

        return runningJobResponse.getObj();
    }

    private List<org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity> getJobs(String site, long currentTime,  String startTime, String endTime) throws Exception{
        GenericEntityServiceResource resource = new GenericEntityServiceResource();
        String query = String.format("%s[@site=\"%s\" and @startTime<=%s and @endTime>%s]{@jobId}", JPA_JOB_EXECUTION_SERVICE_NAME, site, currentTime, currentTime);

        GenericServiceAPIResponseEntity<org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity> response =
                resource.search(query, startTime, endTime, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);

        if (!response.isSuccess() || response.getObj() == null) {
            throw new IOException(response.getException());
        }

        return response.getObj();
    }

    private Map<String, Set<String>> getQueueMap(String site) throws IOException {
        GenericEntityServiceResource resource = new GenericEntityServiceResource();

        String query = String.format("%s[@site=\"%s\"]{*}", QUEUE_MAPPING_SERVICE_NAME, site);
        GenericServiceAPIResponseEntity<QueueStructureAPIEntity> responseEntity = resource.search(query, null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);

        if (!responseEntity.isSuccess() || responseEntity.getObj() == null) {
            throw new IOException(responseEntity.getException());
        }
        Map<String, Set<String>> result = new HashMap<>();
        for(QueueStructureAPIEntity entity : responseEntity.getObj()) {
            String queue = entity.getTags().get("queue");
            Set<String> subQueues = new HashSet<>();
            subQueues.addAll(entity.getAllSubQueues());
            result.put(queue, subQueues);
        }
        return result;
    }

    private Tuple2<String, String> getQueryTimeRange(long currentTime) throws ParseException {
        String startTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTime - DateTimeUtil.ONEHOUR * 12);
        String endTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTime + DateTimeUtil.ONEMINUTE);
        return new Tuple2<>(startTime, endTime);
    }

    private Map<String, Long> getTopRecords(int top, Map<String, Long> map) {
        Map<String, Long> newMap = new LinkedHashMap<>();

        List<Map.Entry<String,Long>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, (o1, o2) -> o1.getValue() >= o2.getValue() ? 1 : -1);
        for (Map.Entry<String, Long> entry : list) {
            if (newMap.size() < top) {
                newMap.put(entry.getKey(), entry.getValue());
            } else {
                break;
            }
        }
        return newMap;
    }
}
