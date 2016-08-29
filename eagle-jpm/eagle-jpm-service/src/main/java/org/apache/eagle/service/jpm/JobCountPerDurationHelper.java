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
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.JobCountPerDurationResponse;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse.UnitJobCount;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class JobCountPerDurationHelper {

    public void initJobCountList(List<UnitJobCount> jobCounts, long startTime, long endTime, long intervalInSecs) {
        for (long i = startTime / intervalInSecs; i * intervalInSecs <= endTime; i++) {
            jobCounts.add(new UnitJobCount(i * intervalInSecs, 0));
        }
    }


    private void fillCounts(List<UnitJobCount> jobCounts, int startIndex, int endIndex) {
        for (int i = startIndex; i <= endIndex && i < jobCounts.size(); i++) {
            jobCounts.get(i).jobCount++;
        }
    }

    public String moveTimeforwardOneDay(String startTime) throws ParseException {
        long timeInSecs = DateTimeUtil.humanDateToSeconds(startTime);
        timeInSecs -= 24L * 60L * 60L;
        return DateTimeUtil.secondsToHumanDate(timeInSecs);
    }

    public JobCountPerDurationResponse getJobCount( List<JobExecutionAPIEntity> jobDurations,
                                                    long startTimeInSecs,
                                                    long endTimeInSecs,
                                                    long intervalInSecs) {
        JobCountPerDurationResponse response = new JobCountPerDurationResponse();
        List<UnitJobCount> jobCounts = new ArrayList<>();
        initJobCountList(jobCounts, startTimeInSecs, endTimeInSecs, intervalInSecs);
        for (JobExecutionAPIEntity jobDuration: jobDurations) {
            countJob(jobCounts, jobDuration.getStartTime() / 1000, jobDuration.getEndTime() / 1000, intervalInSecs);
        }
        response.jobCounts = jobCounts;
        return response;
    }

    public void countJob(List<UnitJobCount> jobCounts, long jobStartTimeSecs, long jobEndTimeSecs, long intervalInSecs) {
        long startCountPoint = jobCounts.get(0).timeBucket;
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
        fillCounts(jobCounts, startIndex, endIndex);
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
