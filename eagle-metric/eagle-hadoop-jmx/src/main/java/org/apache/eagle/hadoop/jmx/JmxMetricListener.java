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

public abstract class JmxMetricListener {
    private OutputCollector collector;
    protected HadoopJmxMonitorConfig config;

    public JmxMetricListener(HadoopJmxMonitorConfig config, OutputCollector collector) {
        this.collector = collector;
        this.config = config;
    }

    public void on_bean(String component, JMXBean bean) {
        return;
    }

    public void on_metric(JmxMetricEntity metric) {
        return;
    }

    public void emit(String streamId, JmxMetricEntity metricEntity) {
        collector.emit(streamId, new Values(PLACE_HOLDER, metricEntity.buildStream()));
    }
}
