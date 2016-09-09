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
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.JobSuggestionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.TaskGroupResponse;

import java.util.ArrayList;
import java.util.List;

public class MapSpillFunc implements SuggestionFunc {

    private static final long SPILL_NORM_FACTOR = 10000;
    private static final double SPILL_RATIO_THRESHOLD = 2.5d;
    private static final double SPILL_RECORDS_THRESHOLD = 100;
    private static final String SPILL_RATIO_NAME_FORMAT = "spillRatio: %s / %s";

    private List<TaskExecutionAPIEntity> getTasks(TaskGroupResponse data) {
        return data.tasksGroupByType.get(Constants.TaskType.MAP.toString()).longTasks;
    }

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        List<TaskExecutionAPIEntity> tasks = getTasks(data);

        long totalSpillRecords = 0;
        long totalOutputRecords = 0;
        double spillRatio = 0;
        for (TaskExecutionAPIEntity task : tasks) {
            totalSpillRecords += task.getJobCounters().getCounterValue(JobCounters.CounterName.SPILLED_RECORDS);
            totalOutputRecords += task.getJobCounters().getCounterValue(JobCounters.CounterName.MAP_OUTPUT_RECORDS);
        }

        if (totalSpillRecords == 0) {
            spillRatio = 0;
        } else {
            spillRatio = (double) totalSpillRecords / totalOutputRecords;
        }

        JobSuggestionResponse response = new JobSuggestionResponse();
        response.suggestionType = Constants.SuggestionType.MapSpill.toString();
        response.suggestionResults = getSpillSuggest(spillRatio);
        response.suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult("average spill records", totalSpillRecords / tasks.size()));
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getSpillSuggest(double spillRatio) {
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        String suggestName = String.format(SPILL_RATIO_NAME_FORMAT, JobCounters.CounterName.SPILLED_RECORDS.getName(), JobCounters.CounterName.MAP_OUTPUT_RECORDS.getName());
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, spillRatio));
        return suggestionResults;
    }
}
