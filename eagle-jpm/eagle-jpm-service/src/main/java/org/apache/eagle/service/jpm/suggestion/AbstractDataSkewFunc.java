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

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.JobSuggestionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.TaskGroupResponse;
import org.apache.eagle.service.jpm.ResourceUtils;

import java.util.ArrayList;
import java.util.List;

abstract public class AbstractDataSkewFunc implements SuggestionFunc {

    private JobCounters.CounterName counterName;
    private Constants.SuggestionType suggestType;
    private static final long DATA_SKEW_THRESHOLD = 8;

    private static final String DEVIATION_SUGGEST_FORMAT = "%s deviation: |avg2 - avg1| / min(avg1, avg2)";
    private static final String AVG1_SUGGEST_FORMAT = "avg1: sum(%s) / %d tasks";
    private static final String AVG2_SUGGEST_FORMAT = "avg2: sum(%s) / %d tasks";
    private static final String DATA_SKEW_SUGGESTION_FORMAT = "%s deviation exceeds threshold %d";

    public AbstractDataSkewFunc(JobCounters.CounterName counterName, Constants.SuggestionType type) {
        this.counterName = counterName;
        this.suggestType = type;
    }

    protected abstract MRTaskExecutionResponse.TaskGroup getTasks(TaskGroupResponse tasks);

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        MRTaskExecutionResponse.TaskGroup taskGroup = getTasks(data);
        double[] smallerGroup = ResourceUtils.getCounterValues(taskGroup.shortTasks, counterName);
        double[] largerGroup = ResourceUtils.getCounterValues(taskGroup.longTasks, counterName);
        DescriptiveStatistics statistics = new DescriptiveStatistics();
        long avgSmaller = (long) statistics.getMeanImpl().evaluate(smallerGroup);
        long avgLarger = (long) statistics.getMeanImpl().evaluate(largerGroup);

        long min = Math.min(avgSmaller, avgLarger);
        long diff = Math.abs(avgLarger - avgSmaller);

        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = getDeviationSuggest(min, diff);
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(String.format(AVG1_SUGGEST_FORMAT, counterName.getName(), taskGroup.shortTasks.size()), avgSmaller));
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(String.format(AVG2_SUGGEST_FORMAT, counterName.getName(), taskGroup.longTasks.size()), avgLarger));
        MRTaskExecutionResponse.JobSuggestionResponse response = new MRTaskExecutionResponse.JobSuggestionResponse();
        response.suggestionResults = suggestionResults;
        response.suggestionType = suggestType.toString();
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getDeviationSuggest(long averageMin, long averageDiff) {
        if (averageMin <= 0) {
            averageMin = 1;
        }
        long deviation = averageDiff / averageMin;
        String suggestName = String.format(DEVIATION_SUGGEST_FORMAT, counterName.getName());
        String suggestion = null;
        if (deviation > DATA_SKEW_THRESHOLD) {
            suggestion = String.format(DATA_SKEW_SUGGESTION_FORMAT, counterName.getName(), DATA_SKEW_THRESHOLD);
        }
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, deviation, suggestion));
        return suggestionResults;
    }
}
