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
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.TaskGroup;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.JobSuggestionResponse;
import org.apache.eagle.service.jpm.MRTaskExecutionResponse.TaskGroupResponse;
import org.apache.eagle.service.jpm.ResourceUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractGCFunc implements SuggestionFunc {

    private static final String GC_RATIO_NAME_FORMAT = "gcRatio (%s / %s) deviation";
    private static final String GC_SUGGESTION_FORMAT = "gcRatio deviation exceeds threshold %.2f, where the deviation is %.2f / %.2f";

    private static final double GC_RATIO_DEVIATION_THRESHOLD = 2;

    private Constants.SuggestionType suggestionType;
    private double threshold;

    public AbstractGCFunc(Constants.SuggestionType suggestionType) {
        this.suggestionType = suggestionType;
        this.threshold = GC_RATIO_DEVIATION_THRESHOLD;
    }

    public AbstractGCFunc(Constants.SuggestionType suggestionType, double threshold) {
        this.suggestionType = suggestionType;
        this.threshold = threshold > 0 ? threshold : GC_RATIO_DEVIATION_THRESHOLD;
    }

    protected abstract TaskGroup getTasks(TaskGroupResponse tasks);


    private double getGcRatio(List<TaskExecutionAPIEntity> tasks) {
        if (tasks.isEmpty()) {
            return 0;
        }
        double[] gcMs = ResourceUtils.getCounterValues(tasks, JobCounters.CounterName.GC_MILLISECONDS);
        double[] cpuMs = ResourceUtils.getCounterValues(tasks, JobCounters.CounterName.CPU_MILLISECONDS);

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        double averageCpuMs = statistics.getMeanImpl().evaluate(cpuMs);
        double averageGcMs = statistics.getMeanImpl().evaluate(gcMs);
        if (averageCpuMs == 0) {
            averageCpuMs = 1;
        }
        return averageGcMs / averageCpuMs;
    }

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        JobSuggestionResponse response = new JobSuggestionResponse();
        response.suggestionType = suggestionType.name();

        TaskGroup taskGroup = getTasks(data);
        if (taskGroup.longTasks.isEmpty()) {
            return response;
        }

        double smallerGcRatio = getGcRatio(taskGroup.shortTasks);
        double largerGcRatio = getGcRatio(taskGroup.longTasks);
        response.suggestionResults = getGCsuggest(smallerGcRatio, largerGcRatio);
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getGCsuggest(double smallerRatio, double largerRatio) {
        if (smallerRatio <= 0) {
            smallerRatio = 1;
        }
        double deviation = largerRatio / smallerRatio;
        String suggestName = String.format(GC_RATIO_NAME_FORMAT, JobCounters.CounterName.GC_MILLISECONDS.getName(), JobCounters.CounterName.CPU_MILLISECONDS.getName());
        String suggestion = null;
        if (deviation > threshold) {
            suggestion = String.format(GC_SUGGESTION_FORMAT, threshold, largerRatio, smallerRatio);
        }
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, deviation, suggestion));
        return suggestionResults;
    }

}
