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
import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRJobTaskCountResponse;
import org.apache.eagle.service.jpm.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMRTaskCountImpl {

    private static final Logger LOG = LoggerFactory.getLogger(TestMRTaskCountImpl.class);
    MRTaskCountImpl helper = new MRTaskCountImpl();

    @Test
    public void test() {
        String timeList = " 0, 10,20,40 ";
        List<Long> times = ResourceUtils.parseDistributionList(timeList);
        Assert.assertTrue(times.size() == 4);

        long val = 25;
        int index = ResourceUtils.getDistributionPosition(times, val);
        Assert.assertTrue(index == 2);
    }

    @Test
    public void test2() {
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

        String timeList = " 0, 10,20,40 ";
        List<Long> times = ResourceUtils.parseDistributionList(timeList);

        helper.initTaskCountList(runningTaskCount, finishedTaskCount, times, new MRTaskCountImpl.RunningTaskComparator());

        for (TaskExecutionAPIEntity o : tasks) {
            int index = ResourceUtils.getDistributionPosition(times, o.getDuration() / DateTimeUtil.ONESECOND);
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
        helper.getTopTasks(runningTaskCount, top);
        Assert.assertTrue(runningTaskCount.get(1).taskCount == 3);
        Assert.assertTrue(runningTaskCount.get(1).topEntities.size() == 2);
    }

    @Test
    public void testCounterParametersParser() {
        String paramString = "HDFS_BYTES_READ:[0,20.0,40], MAP_OUTPUT_BYTES:[20, 40], REDUCE_INPUT_RECORDS: [0, 199], REDUCE_OUTPUT_RECORDS: [3,10]";
        String patternString = "(\\w+):\\s*\\[([\\d,\\.\\s]+)\\]";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(paramString);
        List<String> expectedResults = new ArrayList<>(Arrays.asList("HDFS_BYTES_READ", "0,20.0,40", "MAP_OUTPUT_BYTES", "20, 40", "REDUCE_INPUT_RECORDS", "0, 199", "REDUCE_OUTPUT_RECORDS", "3,10"));
        int i = 0;
        while (matcher.find()) {
            LOG.info(matcher.group(0));
            LOG.info(matcher.group(1));
            LOG.info(matcher.group(2));
            Assert.assertTrue(matcher.group(1).equals(expectedResults.get(i++)));
            Assert.assertTrue(matcher.group(2).equals(expectedResults.get(i++)));
        }
    }

    @Test
    public void testGetCounterName() {
        JobCounters.CounterName counterName = JobCounters.CounterName.valueOf("HDFS_BYTES_READ");
        Assert.assertTrue(counterName.equals(JobCounters.CounterName.HDFS_BYTES_READ));
    }

}
