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

package org.apache.eagle.service.jpm.count;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.JobCountResponse;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.UnitJobCount;
import org.apache.eagle.service.jpm.ResourceUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MRJobCountImpl {

    public void initJobCountList(List<UnitJobCount> jobCounts, long startTime, long endTime, long intervalInSecs) {
        for (long i = startTime / intervalInSecs; i * intervalInSecs <= endTime; i++) {
            jobCounts.add(new UnitJobCount(i * intervalInSecs * DateTimeUtil.ONESECOND));
        }
    }

    public String moveTimeForwardOneDay(String startTime) throws ParseException {
        long timeInSecs = DateTimeUtil.humanDateToSeconds(startTime);
        timeInSecs -= 24L * 60L * 60L;
        return DateTimeUtil.secondsToHumanDate(timeInSecs);
    }

    public JobCountResponse getRunningJobCount(List<JobExecutionAPIEntity> historyJobs,
                                               List<org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity> runningJobs,
                                               long startTimeInSecs,
                                               long endTimeInSecs,
                                               long intervalInSecs) {
        List<UnitJobCount> jobCounts = new ArrayList<>();
        Set<String> jobTypes = new HashSet<>();
        Set<String> jobIds = new HashSet<>();
        initJobCountList(jobCounts, startTimeInSecs, endTimeInSecs, intervalInSecs);
        for (JobExecutionAPIEntity job: historyJobs) {
            jobIds.add(job.getTags().get(MRJobTagName.JOB_ID.toString()));
            String jobType = job.getTags().get(MRJobTagName.JOB_TYPE.toString());
            jobTypes.add(jobType);
            countJob(jobCounts, job.getStartTime() / DateTimeUtil.ONESECOND, job.getEndTime() / DateTimeUtil.ONESECOND, intervalInSecs, jobType);
        }
        for (org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity job : runningJobs) {
            if (!ResourceUtils.isDuplicate(jobIds, job.getTags().get(MRJobTagName.JOB_ID.toString()))) {
                String jobType = job.getTags().get(MRJobTagName.JOB_TYPE.toString());
                jobTypes.add(jobType);
                countJob(jobCounts, job.getStartTime() / DateTimeUtil.ONESECOND, endTimeInSecs, intervalInSecs, jobType);
            }
        }
        JobCountResponse response = new JobCountResponse();
        response.jobCounts = jobCounts;
        response.jobTypes = jobTypes;
        return response;
    }

    public JobCountResponse getHistoryJobCountGroupByDuration(List<JobExecutionAPIEntity> jobDurations, String timeList) {
        JobCountResponse response = new JobCountResponse();
        List<UnitJobCount> jobCounts = new ArrayList<>();
        Set<String> jobTypes = new HashSet<>();
        List<Long> times = ResourceUtils.parseDistributionList(timeList);
        for (int i = 0; i < times.size(); i++) {
            jobCounts.add(new UnitJobCount(times.get(i)));
        }
        for (JobExecutionAPIEntity job : jobDurations) {
            int jobIndex = ResourceUtils.getDistributionPosition(times, job.getDurationTime() / DateTimeUtil.ONESECOND);
            UnitJobCount counter = jobCounts.get(jobIndex);
            String jobType = job.getTags().get(MRJobTagName.JOB_TYPE.toString());
            jobTypes.add(jobType);
            countJob(counter, jobType);
        }
        response.jobCounts = jobCounts;
        response.jobTypes = jobTypes;
        return response;
    }

    public void countJob(UnitJobCount counter, String jobType) {
        if (null  ==  jobType) {
            jobType = "null";
        }
        counter.jobCount++;
        if (counter.jobCountByType.containsKey(jobType)) {
            counter.jobCountByType.put(jobType, counter.jobCountByType.get(jobType) + 1);
        } else {
            counter.jobCountByType.put(jobType, 1L);
        }
    }

    public void countJob(List<UnitJobCount> jobCounts, long jobStartTimeSecs, long jobEndTimeSecs, long intervalInSecs, String jobType) {
        long startCountPoint = jobCounts.get(0).timeBucket / DateTimeUtil.ONESECOND;
        if (jobEndTimeSecs < startCountPoint) {
            return;
        }
        int startIndex = 0;
        if (jobStartTimeSecs > startCountPoint) {
            long relativeStartTime = jobStartTimeSecs - startCountPoint;
            startIndex = (int) (relativeStartTime / intervalInSecs) + (relativeStartTime % intervalInSecs == 0 ? 0 : 1);
        }
        long relativeEndTime = jobEndTimeSecs - startCountPoint;
        int endIndex = (int) (relativeEndTime / intervalInSecs);

        for (int i = startIndex; i <= endIndex && i < jobCounts.size(); i++) {
            countJob(jobCounts.get(i), jobType);
        }
    }

    public List<String> getSearchTimeDuration(List<JobExecutionAPIEntity> jobEntities) {
        List<String> pair = new ArrayList<>();
        long minStartTime = System.currentTimeMillis();
        long maxEndTime = 0;
        for (JobExecutionAPIEntity jobEntity : jobEntities) {
            if (minStartTime > jobEntity.getStartTime()) {
                minStartTime = jobEntity.getStartTime();
            }
            if (maxEndTime < jobEntity.getEndTime()) {
                maxEndTime = jobEntity.getEndTime();
            }
        }
        pair.add(DateTimeUtil.millisecondsToHumanDateWithSeconds(minStartTime));
        pair.add(DateTimeUtil.millisecondsToHumanDateWithSeconds(maxEndTime));
        return pair;
    }
}
