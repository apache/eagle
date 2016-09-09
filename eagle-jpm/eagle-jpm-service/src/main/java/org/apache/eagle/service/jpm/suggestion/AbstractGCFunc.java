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
import org.apache.eagle.common.DateTimeUtil;
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

    private static final String GC_RATIO_NAME_FORMAT = "gcRatio: %s / %s";

    private static final double GC_RATIO_THRESHOLD = 0.03d;
    private static final long RUNTIME_IN_MIN_THRESHOLD = 12;

    private Constants.SuggestionType suggestionType;

    public AbstractGCFunc(Constants.SuggestionType suggestionType) {
        this.suggestionType = suggestionType;
    }

    protected abstract TaskGroup getTasks(TaskGroupResponse tasks);

    @Override
    public JobSuggestionResponse apply(TaskGroupResponse data) {
        JobSuggestionResponse response = new JobSuggestionResponse();
        response.suggestionType = suggestionType.name();

        TaskGroup taskGroup = getTasks(data);
        if (taskGroup.longTasks.isEmpty()) {
            return response;
        }
        double[] gcMs = ResourceUtils.getCounterValues(taskGroup.longTasks, JobCounters.CounterName.GC_MILLISECONDS);
        double[] cpuMs = ResourceUtils.getCounterValues(taskGroup.longTasks, JobCounters.CounterName.CPU_MILLISECONDS);
        List<Double> runtimeList = new ArrayList<>();
        taskGroup.longTasks.forEach(o -> runtimeList.add(Double.valueOf(o.getDuration())));
        double[] runtimeMs = ResourceUtils.toArray(runtimeList);

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        long averageRuntimeMs = (long) statistics.getMeanImpl().evaluate(runtimeMs);
        long averageCpuMs = (long) statistics.getMeanImpl().evaluate(cpuMs);
        long averageGcMs = (long) statistics.getMeanImpl().evaluate(gcMs);

        response.suggestionResults = getGCsuggest(averageCpuMs, averageGcMs, averageRuntimeMs);
        response.suggestionResults.add(getRuntimeSuggest(averageRuntimeMs));
        return response;
    }

    private List<MRTaskExecutionResponse.SuggestionResult> getGCsuggest(long avgCpuMs, long avgGcMs, long avgRuntimeMs) {
        List<MRTaskExecutionResponse.SuggestionResult> suggestionResults = new ArrayList<>();
        double gcRatio = avgGcMs * 1d / avgCpuMs;
        String suggestName = String.format(GC_RATIO_NAME_FORMAT, JobCounters.CounterName.GC_MILLISECONDS.getName(), JobCounters.CounterName.CPU_MILLISECONDS.getName());
        String suggestion = null;
        if (gcRatio > GC_RATIO_THRESHOLD) {
            suggestion = "gcRatio exceeds threshold " + GC_RATIO_THRESHOLD;
        }
        suggestionResults.add(new MRTaskExecutionResponse.SuggestionResult(suggestName, gcRatio, suggestion));
        suggestionResults.add(getRuntimeSuggest(avgRuntimeMs));
        return suggestionResults;
    }

    private MRTaskExecutionResponse.SuggestionResult getRuntimeSuggest(long avgRuntimeMs) {
        String suggestion = null;
        if (avgRuntimeMs > RUNTIME_IN_MIN_THRESHOLD * DateTimeUtil.ONEMINUTE) {
            suggestion = String.format("average task duration exceeds threshold %d minutes", RUNTIME_IN_MIN_THRESHOLD);
        }
        return new MRTaskExecutionResponse.SuggestionResult("average task duration", avgRuntimeMs, suggestion);
    }
}
