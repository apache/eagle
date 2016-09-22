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

public class TestDataSkewFunc {
    MRTaskExecutionResource resource = new MRTaskExecutionResource();

    public TaskExecutionAPIEntity createTestTask(String taskType, long duration, long hdfsReadByte, long reduceShuffleByte) {
        TaskExecutionAPIEntity task = new TaskExecutionAPIEntity();
        task.setDuration(duration);
        task.setTags(new HashMap<String, String>());
        task.getTags().put(MRJobTagName.TASK_TYPE.toString(), taskType);
        JobCounters jobCounters = new JobCounters();
        Map<String, Map<String, Long>> counters = new HashMap<>();
        Map<String, Long> taskCounters = new HashMap<>();
        taskCounters.put(JobCounters.CounterName.HDFS_BYTES_READ.getName(), hdfsReadByte);
        taskCounters.put(JobCounters.CounterName.REDUCE_SHUFFLE_BYTES.getName(), reduceShuffleByte);

        if (hdfsReadByte != 0) {
            counters.put(JobCounters.GroupName.FileSystemCounters.getName(), taskCounters);
        }
        if (reduceShuffleByte != 0) {
            counters.put(JobCounters.GroupName.MapReduceTaskCounter.getName(), taskCounters);
        }
        jobCounters.setCounters(counters);
        task.setJobCounters(jobCounters);
        return task;
    }

    @Test
    public void testDataSkewFunc() {
        MRTaskExecutionResponse.TaskGroupResponse response = new MRTaskExecutionResponse.TaskGroupResponse();
        response.tasksGroupByType = new HashMap<>();
        response.tasksGroupByType.put(Constants.TaskType.MAP.toString(), new MRTaskExecutionResponse.TaskGroup());
        response.tasksGroupByType.put(Constants.TaskType.REDUCE.toString(), new MRTaskExecutionResponse.TaskGroup());

        List<TaskExecutionAPIEntity> tasks = new ArrayList<>();
        tasks.add(createTestTask(Constants.TaskType.MAP.toString(), 9480, 327, 12860L));
        tasks.add(createTestTask(Constants.TaskType.MAP.toString(), 27619, 379682, 379682));
        tasks.add(createTestTask(Constants.TaskType.REDUCE.toString(), 19699, 45, 908));
        tasks.add(createTestTask(Constants.TaskType.REDUCE.toString(), 35688, 45, 293797));
        tasks.add(createTestTask(Constants.TaskType.REDUCE.toString(), 31929, 45, 2062520));
        MRTaskExecutionResponse.TaskGroupResponse taskGroup = resource.groupTasksByValue(response, true, tasks, 10000);

        List<MRTaskExecutionResponse.JobSuggestionResponse> result = new ArrayList<>();

        List<SuggestionFunc> suggestionFuncs = new ArrayList<>();
        suggestionFuncs.add(new MapInputFunc());
        suggestionFuncs.add(new ReduceInputFunc());
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
