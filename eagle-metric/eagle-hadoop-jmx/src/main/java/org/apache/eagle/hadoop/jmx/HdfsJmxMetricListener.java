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
import org.apache.eagle.hadoop.jmx.model.JmxMetricEntity;

import java.util.List;
import java.util.Objects;

public class HdfsJmxMetricListener extends JmxMetricListener {
    private List<JmxMetricListener> listeners;
    private OutputCollector collector;
    private HadoopJmxMonitorConfig config;

    public HdfsJmxMetricListener(OutputCollector collector, HadoopJmxMonitorConfig config) {
        super(config, collector);
        this.collector = collector;
        this.config = config;
    }

    public void registerListeners() {
        register(new NNSafeModeMetric(collector, config));

    }

    private void register(JmxMetricListener listener) {
        listeners.add(listener);
    }

    public void on_bean(String component, JMXBean bean) {
        listeners.forEach(listener -> listener.on_bean(component, bean));
    }

    public void on_metric(JmxMetricEntity metric) {
        listeners.forEach(listener -> listener.on_metric(metric));
    }

    private class NNSafeModeMetric extends JmxMetricListener {

        public NNSafeModeMetric(OutputCollector collector, HadoopJmxMonitorConfig config) {
            super(config, collector);
        }

        public void on_metric(JmxMetricEntity metric) {
            if (metric.getMetric().equals("hadoop.namenode.fsnamesystemstate.fsstate")) {
                if (Objects.equals(metric.getValue(), "safeMode")) {
                    metric.setValue(1);
                } else {
                    metric.setValue(0);
                }
            }
            emit(config.JMX_METRCI_STREAM, metric);
        }
    }

    private class NNHAMetric extends JmxMetricListener {

        public NNHAMetric(HadoopJmxMonitorConfig config, OutputCollector collector) {
            super(config, collector);
        }

        public void on_bean(String component, JMXBean bean) {
            String PREFIX = "hadoop.namenode.fsnamesystem";
            if
        }


    }
 }
