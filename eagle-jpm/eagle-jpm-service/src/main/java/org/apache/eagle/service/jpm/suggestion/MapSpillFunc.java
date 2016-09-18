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

    private static final double SPILL_DEVIATION_THRESHOLD = 2;

    private static final String SPILL_RATIO_NAME_FORMAT = "average %s deviation";
    private static final String SPILL_SUGGESTION_FORMAT = "map spill deviation exceeds threshold %.2f, where the deviation is %.2f / %.2f";

    private double threshold;

    public MapSpillFunc() {
        this.threshold = SPILL_DEVIATION_THRESHOLD;
    }

    public MapSpillFunc(double threshold) {
        this.threshold = threshold > 0 ? threshold : SPILL_DEVIATION_THRESHOLD;
    }

    private MRTaskExecutionResponse.TaskGroup getTasks(TaskGroupResponse data) {
        return data.tasksGroupByType.get(Constants.TaskType.MAP.toString());
    }

    private double getAverageSpillBytes(List<TaskExecutionAPIEntity> tasks) {
        if (tasks.isEmpty()) {
            return 0;
        }
        long totalSpillBytes = 0;
        for (TaskExecutionAPIEntity task : tasks) {
            totalSpillBytes += task.getJobCounters().getCounterValue(JobCounters.CounterName.SPLIT_RAW_BYTES);
        }
        return totalSpillBytes / tasks.size();
    }

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        MRTaskExecutionResponse.TaskGroup tasks = getTasks(data);

        double smallerSpillBytes = getAverageSpillBytes(tasks.shortTasks);
        double largerSpillBytes = getAverageSpillBytes(tasks.longTasks);

        JobSuggestionResponse response = new JobSuggestionResponse();
        response.suggestionType = Constants.SuggestionType.MapSpill.toString();
        response.suggestionResults = getSpillSuggest(smallerSpillBytes, largerSpillBytes);
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getSpillSuggest(double smallerSpillBytes, double largerSpillBytes) {
        if (smallerSpillBytes == 0) {
            smallerSpillBytes = 1;
        }
        double deviation = largerSpillBytes / smallerSpillBytes;
        String suggestion = null;
        if (deviation > threshold) {
            suggestion = String.format(SPILL_SUGGESTION_FORMAT, threshold, largerSpillBytes, smallerSpillBytes);
        }
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        String suggestName = String.format(SPILL_RATIO_NAME_FORMAT, JobCounters.CounterName.SPLIT_RAW_BYTES.getName());
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, deviation, suggestion));
        return suggestionResults;
    }
}
