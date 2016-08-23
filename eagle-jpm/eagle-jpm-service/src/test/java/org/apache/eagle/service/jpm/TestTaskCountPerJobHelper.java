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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TestTaskCountPerJobHelper {
    TaskCountPerJobHelper helper = new TaskCountPerJobHelper();
    @Test
    public void test() {
        String timeList = " 0, 10,20,40 ";
        List<Long> times = helper.parseTimeList(timeList);
        Assert.assertTrue(times.size() == 4);

        long val = 25 * 1000;
        int index = helper.getPosition(times, val);
        Assert.assertTrue(index == 2);
    }

    @Test
    public void test2() {
        String timeList = " 0, 10,20,40 ";
        List<Long> times = helper.parseTimeList(timeList);

        TaskExecutionAPIEntity test1 = new TaskExecutionAPIEntity();
        test1.setDuration(15 * 1000);
        test1.setTaskStatus("running");
        TaskExecutionAPIEntity test4 = new TaskExecutionAPIEntity();
        test4.setDuration(13 * 1000);
        test4.setTaskStatus("running");
        TaskExecutionAPIEntity test2 = new TaskExecutionAPIEntity();
        test2.setDuration(0 * 1000);
        test2.setEndTime(100);
        test2.setTaskStatus("x");
        TaskExecutionAPIEntity test3 = new TaskExecutionAPIEntity();
        test3.setDuration(19 * 1000);
        test3.setTaskStatus("running");
        TaskExecutionAPIEntity test5 = new TaskExecutionAPIEntity();
        test5.setDuration(20 * 1000);
        test5.setEndTime(28);
        test5.setTaskStatus("x");
        List<TaskExecutionAPIEntity> tasks = new ArrayList<>();
        tasks.add(test1);
        tasks.add(test2);
        tasks.add(test3);
        tasks.add(test4);
        tasks.add(test5);

        List<MRJobTaskCountResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        List<MRJobTaskCountResponse.UnitTaskCount> finishedTaskCount = new ArrayList<>();

        helper.initTaskCountList(runningTaskCount, finishedTaskCount, times, new TaskCountPerJobHelper.RunningTaskComparator());

        for (TaskExecutionAPIEntity o : tasks) {
            int index = helper.getPosition(times, o.getDuration());
            if (o.getTaskStatus().equalsIgnoreCase(Constants.TaskState.RUNNING.toString())) {
                MRJobTaskCountResponse.UnitTaskCount counter = runningTaskCount.get(index);
                counter.taskCount++;
                counter.entities.add(o);
            } else if (o.getEndTime() != 0) {
                MRJobTaskCountResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                counter.taskCount++;
                counter.entities.add(o);
            }
        }
        int top = 2;
        if (top > 0)  {
            helper.getTopTasks(runningTaskCount, top);
        }
        Assert.assertTrue(runningTaskCount.get(1).taskCount == 3);
        Assert.assertTrue(runningTaskCount.get(1).topEntities.size() == 2);
    }
}
