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

import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class TaskCountPerJobHelper {

    private final static Logger LOG = LoggerFactory.getLogger(TaskCountPerJobHelper.class);

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

    public void getTopTasks(List<MRJobTaskCountResponse.UnitTaskCount> list, long top) {
        for (MRJobTaskCountResponse.UnitTaskCount taskCounter : list) {
            Iterator<TaskExecutionAPIEntity> iterator = taskCounter.entities.iterator();
            for (int i = 0; i < top && iterator.hasNext(); i++) {
                taskCounter.topEntities.add(iterator.next());
            }
            taskCounter.entities.clear();
        }
    }

    public void taskCount(MRJobTaskCountResponse.UnitTaskCount counter, String taskType) {
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
