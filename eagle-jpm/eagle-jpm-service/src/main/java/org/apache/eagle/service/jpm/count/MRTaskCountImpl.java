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

import org.apache.commons.io.FileUtils;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse;
import org.apache.eagle.service.jpm.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MRTaskCountImpl {

    private static final Logger LOG = LoggerFactory.getLogger(MRTaskCountImpl.class);

    public void getTopTasks(List<MRJobTaskCountResponse.UnitTaskCount> list, long top) {
        for (MRJobTaskCountResponse.UnitTaskCount taskCounter : list) {
            Iterator<TaskExecutionAPIEntity> iterator = taskCounter.entities.iterator();
            for (int i = 0; i < top && iterator.hasNext(); i++) {
                taskCounter.topEntities.add(iterator.next());
            }
            taskCounter.entities.clear();
        }
    }

    public void countTask(MRJobTaskCountResponse.UnitTaskCount counter, String taskType) {
        counter.taskCount++;
        if (taskType.equalsIgnoreCase(Constants.TaskType.MAP.toString())) {
            counter.mapTaskCount++;
        } else if (taskType.equalsIgnoreCase(Constants.TaskType.REDUCE.toString())) {
            counter.reduceTaskCount++;
        }
    }

    public void initTaskCountList(List<MRJobTaskCountResponse.UnitTaskCount> runningTaskCount,
                                  List<MRJobTaskCountResponse.UnitTaskCount> finishedTaskCount,
                                  List<Long> times,
                                  Comparator comparator) {
        for (int i = 0; i < times.size(); i++) {
            runningTaskCount.add(new MRJobTaskCountResponse.UnitTaskCount(times.get(i), comparator));
            finishedTaskCount.add(new MRJobTaskCountResponse.UnitTaskCount(times.get(i), comparator));
        }
    }

    public static class RunningTaskComparator implements Comparator<TaskExecutionAPIEntity> {
        @Override
        public int compare(TaskExecutionAPIEntity o1, TaskExecutionAPIEntity o2) {
            Long time1 = o1.getDuration();
            Long time2 = o2.getDuration();
            return (time1 > time2 ? -1 : (time1.equals(time2)) ? 0 : 1);
        }
    }

    public static class HistoryTaskComparator implements Comparator<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> {
        @Override
        public int compare(org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o1,
                           org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity o2) {
            Long time1 = o1.getDuration();
            Long time2 = o2.getDuration();
            return (time1 > time2 ? -1 : (time1.equals(time2)) ? 0 : 1);
        }
    }

    public MRJobTaskCountResponse.HistoryTaskCountResponse countHistoryTask(List<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> tasks, long startTimeInMin, long endTimeInMin) {
        List<MRJobTaskCountResponse.UnitTaskCount> taskCounts = new ArrayList<>();
        for (long i = startTimeInMin; i <= endTimeInMin; i++) {
            taskCounts.add(new MRJobTaskCountResponse.UnitTaskCount(i * DateTimeUtil.ONEMINUTE, null));
        }
        for (org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity task : tasks) {
            String taskType = task.getTags().get(MRJobTagName.TASK_TYPE.toString());
            long taskStarTimeMin = task.getStartTime() / DateTimeUtil.ONEMINUTE;
            long taskEndTimeMin = task.getEndTime() / DateTimeUtil.ONEMINUTE;
            int relativeStartTime = (int) (taskStarTimeMin - startTimeInMin);
            int relativeEndTime = (int) (taskEndTimeMin - startTimeInMin);
            for (int i = relativeStartTime; i <= relativeEndTime; i++) {
                countTask(taskCounts.get(i), taskType);
            }
        }
        MRJobTaskCountResponse.HistoryTaskCountResponse response = new MRJobTaskCountResponse.HistoryTaskCountResponse();
        response.taskCount = taskCounts;
        return response;
    }

    public MRTaskExecutionResponse.TaskDistributionResponse getHistoryTaskDistribution(List<org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity> tasks, String counterName, String distRange) {
        MRTaskExecutionResponse.TaskDistributionResponse response = new MRTaskExecutionResponse.TaskDistributionResponse();
        response.counterName = counterName;
        List<Long> distRangeList = ResourceUtils.parseDistributionList(distRange);
        for (int i = 0; i < distRangeList.size(); i++) {
            response.taskBuckets.add(new MRTaskExecutionResponse.CountUnit(distRangeList.get(i)));
        }
        JobCounters.CounterName jobCounterName = JobCounters.CounterName.valueOf(counterName.toUpperCase());
        for (org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity task : tasks) {
            Long counterValue = task.getJobCounters().getCounterValue(jobCounterName);
            int pos = ResourceUtils.getDistributionPosition(distRangeList, counterValue);
            response.taskBuckets.get(pos).countVal++;
        }
        return response;
    }


}
