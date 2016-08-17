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
import java.util.List;

public class TestMRJobExecutionResource {

    @Test
    public void test() {
        MRJobExecutionResource resource = new MRJobExecutionResource();
        String timeList = " 0, 10,20,40 ";
        List<Long> times = resource.parseTimeList(timeList);
        Assert.assertTrue(times.size() == 4);

        long val = 25;
        int index = resource.getPosition(times, val);
        Assert.assertTrue(index == 2);

        List<MRJobTaskGroupResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        for (int i = 0; i < times.size(); i++) {
            runningTaskCount.add(new MRJobTaskGroupResponse.UnitTaskCount(times.get(i)));
        }
        Assert.assertTrue(runningTaskCount.get(0) != null);
    }

    @Test
    public void test2() {
        MRJobExecutionResource resource = new MRJobExecutionResource();
        String timeList = " 0, 10,20,40 ";
        List<Long> times = resource.parseTimeList(timeList);

        TaskExecutionAPIEntity test1 = new TaskExecutionAPIEntity();
        test1.setElapsedTime(15);
        test1.setStatus("running");
        TaskExecutionAPIEntity test4 = new TaskExecutionAPIEntity();
        test4.setElapsedTime(13);
        test4.setStatus("running");
        TaskExecutionAPIEntity test2 = new TaskExecutionAPIEntity();
        test2.setElapsedTime(0);
        test2.setFinishTime(100);
        test2.setStatus("x");
        TaskExecutionAPIEntity test3 = new TaskExecutionAPIEntity();
        test3.setElapsedTime(19);
        test3.setStatus("running");
        TaskExecutionAPIEntity test5 = new TaskExecutionAPIEntity();
        test5.setElapsedTime(20);
        test5.setFinishTime(28);
        test5.setStatus("x");
        List<TaskExecutionAPIEntity> tasks = new ArrayList<>();
        tasks.add(test1);
        tasks.add(test2);
        tasks.add(test3);
        tasks.add(test4);
        tasks.add(test5);

        List<MRJobTaskGroupResponse.UnitTaskCount> runningTaskCount = new ArrayList<>();
        List<MRJobTaskGroupResponse.UnitTaskCount> finishedTaskCount = new ArrayList<>();

        for (int i = 0; i < times.size(); i++) {
            runningTaskCount.add(new MRJobTaskGroupResponse.UnitTaskCount(times.get(i)));
            finishedTaskCount.add(new MRJobTaskGroupResponse.UnitTaskCount(times.get(i)));
        }

        for (TaskExecutionAPIEntity o : tasks) {
            int index = resource.getPosition(times, o.getElapsedTime());

            if (o.getStatus().equalsIgnoreCase(Constants.TaskState.RUNNING.toString())) {
                MRJobTaskGroupResponse.UnitTaskCount counter = runningTaskCount.get(index);
                counter.taskCount++;
                counter.entities.add(o);
            } else if (o.getFinishTime() != 0) {
                MRJobTaskGroupResponse.UnitTaskCount counter = finishedTaskCount.get(index);
                counter.taskCount++;
                counter.entities.add(o);
            }
        }
        int top = 2;
        if (top > 0)  {
            resource.getTopTasks(runningTaskCount, top);
        }
        Assert.assertTrue(runningTaskCount.get(1).taskCount == 3);
        Assert.assertTrue(runningTaskCount.get(1).topEntities.size() == 2);
    }
}
