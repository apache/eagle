/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.jpm.mr.history.metrics;

import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.metrics.AbstractMetricsCreationListener;
import org.apache.eagle.log.entity.GenericMetricEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobExecutionMetricsCreationListener extends AbstractMetricsCreationListener<JobExecutionAPIEntity> {

    @Override
    public List<GenericMetricEntity> generateMetrics(JobExecutionAPIEntity entity) {
        List<GenericMetricEntity> metrics = new ArrayList<>();
        if (entity != null) {
            Long timeStamp = entity.getTimestamp();
            Map<String, String> tags = entity.getTags();
            metrics.add(metricWrapper(timeStamp,
                Constants.JOB_EXECUTION_TIME,
                new double[]{entity.getDurationTime()},
                tags));

            metrics.add(metricWrapper(
                timeStamp,
                Constants.MAP_COUNT_RATIO,
                new double[]{entity.getNumTotalMaps(), 1.0 * entity.getNumFailedMaps() / entity.getNumTotalMaps()},
                tags));

            metrics.add(metricWrapper(
                timeStamp,
                Constants.REDUCE_COUNT_RATIO,
                new double[]{entity.getNumTotalReduces(), 1.0 * entity.getNumFailedReduces() / entity.getNumTotalReduces()},
                tags));

            org.apache.eagle.jpm.util.jobcounter.JobCounters jobCounters = entity.getJobCounters();
            if (jobCounters != null && jobCounters.getCounters() != null) {
                for (Map<String, Long> metricGroup : jobCounters.getCounters().values()) {
                    for (Map.Entry<String, Long> entry : metricGroup.entrySet()) {
                        String metricName = entry.getKey().toLowerCase();
                        metrics.add(metricWrapper(timeStamp, metricName, new double[]{entry.getValue()}, tags));
                    }
                }
            }
        }
        return metrics;
    }

    @Override
    public String buildMetricName(String field) {
        return String.format(Constants.HADOOP_HISTORY_TOTAL_METRIC_FORMAT, Constants.JOB_LEVEL, field);
    }


}

