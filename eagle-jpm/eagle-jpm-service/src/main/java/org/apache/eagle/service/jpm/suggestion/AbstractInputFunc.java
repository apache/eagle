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

public abstract class AbstractInputFunc implements SuggestionFunc {

    private JobCounters.CounterName counterName;
    private Constants.SuggestionType suggestType;
    private double threshold;
    private static final long DATA_SKEW_THRESHOLD = 2;

    private static final String DEVIATION_SUGGEST_FORMAT = "average %s deviation";
    private static final String DATA_SKEW_SUGGESTION_FORMAT = "%s deviation exceeds threshold %.2f, where the deviation is %.2f / %.2f";

    public AbstractInputFunc(JobCounters.CounterName counterName, Constants.SuggestionType type) {
        this.counterName = counterName;
        this.suggestType = type;
        this.threshold = DATA_SKEW_THRESHOLD;
    }

    public AbstractInputFunc(JobCounters.CounterName counterName, Constants.SuggestionType type, double threshold) {
        this.counterName = counterName;
        this.suggestType = type;
        this.threshold = threshold > 0 ? threshold : DATA_SKEW_THRESHOLD;
    }

    protected abstract MRTaskExecutionResponse.TaskGroup getTasks(TaskGroupResponse tasks);

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        MRTaskExecutionResponse.TaskGroup taskGroup = getTasks(data);
        double[] smallerGroup = ResourceUtils.getCounterValues(taskGroup.shortTasks, counterName);
        double[] largerGroup = ResourceUtils.getCounterValues(taskGroup.longTasks, counterName);
        DescriptiveStatistics statistics = new DescriptiveStatistics();
        double avgSmaller = statistics.getMeanImpl().evaluate(smallerGroup);
        double avgLarger = statistics.getMeanImpl().evaluate(largerGroup);

        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = getDeviationSuggest(avgSmaller, avgLarger);
        MRTaskExecutionResponse.JobSuggestionResponse response = new MRTaskExecutionResponse.JobSuggestionResponse();
        response.suggestionResults = suggestionResults;
        response.suggestionType = suggestType.toString();
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getDeviationSuggest(double avgSmaller, double avgLarger) {
        if (avgSmaller <= 0) {
            avgSmaller = 1;
        }
        double deviation = avgLarger / avgSmaller;
        String suggestName = String.format(DEVIATION_SUGGEST_FORMAT, counterName.getName());
        String suggestion = null;
        if (deviation > threshold) {
            suggestion = String.format(DATA_SKEW_SUGGESTION_FORMAT, counterName.getName(), threshold, avgLarger, avgSmaller);
        }
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, deviation, suggestion));
        return suggestionResults;
    }
}
