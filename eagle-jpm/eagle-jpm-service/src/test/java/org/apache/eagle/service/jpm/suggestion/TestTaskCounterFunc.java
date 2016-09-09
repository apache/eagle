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

package org.apache.eagle.service.jpm.suggestion;

import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRTaskExecutionResource;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestTaskCounterFunc {
    MRTaskExecutionResource resource = new MRTaskExecutionResource();

    public TaskExecutionAPIEntity createTestTask(String taskType, long duration, long gcMs, long cpuMs, long spillRecords, long mapOutput) {
        TaskExecutionAPIEntity task = new TaskExecutionAPIEntity();
        task.setDuration(duration);
        task.setTags(new HashMap<>());
        task.getTags().put(MRJobTagName.TASK_TYPE.toString(), taskType);
        JobCounters jobCounters = new JobCounters();
        Map<String, Map<String, Long>> counters = new HashMap<>();
        Map<String, Long> taskCounters = new HashMap<>();
        taskCounters.put(JobCounters.CounterName.GC_MILLISECONDS.getName(), gcMs);
        taskCounters.put(JobCounters.CounterName.CPU_MILLISECONDS.getName(), cpuMs);
        taskCounters.put(JobCounters.CounterName.SPILLED_RECORDS.getName(), spillRecords);
        taskCounters.put(JobCounters.CounterName.MAP_OUTPUT_RECORDS.getName(), mapOutput);

        counters.put(JobCounters.GroupName.MapReduceTaskCounter.getName(), taskCounters);
        jobCounters.setCounters(counters);
        task.setJobCounters(jobCounters);
        return task;
    }

    @Test
    public void testTaskGroupByValue() {
        List<TaskExecutionAPIEntity> tasks = new ArrayList<>();
        tasks.add(createTestTask(Constants.TaskType.MAP.toString(), 2132259, 300L, 12860L, 0L, 2091930L));
        tasks.add(createTestTask(Constants.TaskType.MAP.toString(), 42071, 800, 21010L, 0L, 2092547L));
        tasks.add(createTestTask(Constants.TaskType.REDUCE.toString(), 19699, 300, 3320, 0L, 0L));
        MRTaskExecutionResponse.TaskGroupResponse taskGroup = resource.groupTasksByValue(tasks, 30000);


        List<MRTaskExecutionResponse.JobSuggestionResponse> result = new ArrayList<>();

        List<SuggestionFunc> suggestionFuncs = new ArrayList<>();
        suggestionFuncs.add(new MapGCFunc());
        suggestionFuncs.add(new ReduceGCFunc());
        suggestionFuncs.add(new MapSpillFunc());
        try {
            for (SuggestionFunc func : suggestionFuncs) {
                result.add(func.apply(taskGroup));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        Assert.assertTrue(result.size() > 0);
    }

}
