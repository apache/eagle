/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.jmx;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import org.apache.eagle.hadoop.jmx.model.JmxMetricEntity;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.PLACE_HOLDER;

public class JmxMetricCollector {
    private HadoopJmxMonitorConfig config;
    private OutputCollector collector;

    public JmxMetricCollector(HadoopJmxMonitorConfig config, OutputCollector collector) {
        this.config = config;
        this.collector = collector;
    }

    public void emit(JmxMetricEntity baseMetric, String metricName, Object value) {
        emit(baseMetric, metricName, value, HadoopJmxConstant.MetricType.METRIC);
    }

    public void emit(JmxMetricEntity baseMetric, String metricName, Object value, HadoopJmxConstant.MetricType type) {
        if (config.filterMetrics.contains(metricName)) {
            baseMetric.setMetric(metricName.toLowerCase());
            baseMetric.setValue(value);
            if (type.equals(HadoopJmxConstant.MetricType.METRIC)) {
                collector.emit(config.JMX_METRCI_STREAM, new Values(PLACE_HOLDER, baseMetric.buildStream()));
            } else {
                collector.emit(config.JMX_RESOURCE_METRIC_STRAEM, new Values(PLACE_HOLDER, baseMetric.buildStream()));
            }
        }
    }

}
