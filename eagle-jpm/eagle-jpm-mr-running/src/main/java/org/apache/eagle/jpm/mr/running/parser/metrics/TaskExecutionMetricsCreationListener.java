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

package org.apache.eagle.jpm.mr.running.parser.metrics;

import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.metrics.AbstractMetricsCreationListener;
import org.apache.eagle.log.entity.GenericMetricEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TaskExecutionMetricsCreationListener extends AbstractMetricsCreationListener<TaskExecutionAPIEntity> {
    @Override
    public List<GenericMetricEntity> generateMetrics(TaskExecutionAPIEntity entity) {
        List<GenericMetricEntity> metrics = new ArrayList<>();
        if (entity != null) {
            Long currentTime = System.currentTimeMillis();
            Map<String, String> tags = entity.getTags();
            metrics.add(metricWrapper(currentTime, Constants.TASK_EXECUTION_TIME, new double[]{entity.getDuration()}, tags));
        }
        return metrics;
    }

    @Override
    public String buildMetricName(String field) {
        return String.format(Constants.hadoopMetricFormat, Constants.TASK_LEVEL, field);
    }
}
