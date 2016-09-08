/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.runner;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.metric.IMetricSystem;
import org.apache.eagle.alert.metric.MetricSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

import com.codahale.metrics.Gauge;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;

/**
 * Per MetricSystem instance per task
 */
public class StormMetricTaggedConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(StormMetricTaggedConsumer.class);
    private String topologyName;
    private Map<String, MetricSystem> metricSystems;
    private String stormId;
    private Config config;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        this.config = ConfigFactory.parseString((String) registrationArgument, ConfigParseOptions.defaults());
        topologyName = config.getString("topology.name");
        stormId = context.getStormId();
        metricSystems = new HashMap<>();
    }

    @SuppressWarnings("serial")
    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        synchronized (metricSystems) {
            String uniqueTaskKey = buildUniqueTaskKey(taskInfo);
            MetricSystem metricSystem = metricSystems.get(uniqueTaskKey);
            if (metricSystem == null) {
                metricSystem = MetricSystem.load(config);
                metricSystems.put(uniqueTaskKey, metricSystem);
                metricSystem.tags(new HashMap<String, Object>() {{
                    put("topology", topologyName);
                    put("stormId", stormId);
                    put("component", taskInfo.srcComponentId);
                    put("task", taskInfo.srcTaskId);
                }});
                metricSystem.start();
                LOG.info("Initialized metric reporter for {}", uniqueTaskKey);
            }
            report(metricSystem, taskInfo, dataPoints);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reported {} metric points from {}", dataPoints.size(), uniqueTaskKey);
            }
        }
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    private void report(MetricSystem metricSystem, TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        List<String> metricList = new LinkedList<>();
        for (DataPoint dataPoint : dataPoints) {
            if (dataPoint.value instanceof Map) {
                Map<String, Object> values = (Map<String, Object>) dataPoint.value;
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    String metricName = buildSimpleMetricName(taskInfo, dataPoint.name, entry.getKey());
                    metricList.add(metricName);
                    Gauge gauge = metricSystem.registry().getGauges().get(metricName);
                    if (gauge == null) {
                        gauge = new DataPointGauge(entry.getValue());
                        metricSystem.registry().register(metricName, gauge);
                        LOG.debug("Register metric {}", metricName);
                    } else {
                        ((DataPointGauge) gauge).setValue(entry.getValue());
                    }
                }
            } else {
                String metricName = buildSimpleMetricName(taskInfo, dataPoint.name);
                metricList.add(metricName);
                Gauge gauge = metricSystem.registry().getGauges().get(metricName);
                if (gauge == null) {
                    gauge = new DataPointGauge(dataPoint.value);
                    metricSystem.registry().register(metricName, gauge);
                    LOG.debug("Register metric {}", metricName);
                } else {
                    ((DataPointGauge) gauge).setValue(dataPoint.value);
                }
            }
        }
        metricSystem.registry().removeMatching((name, metric) -> metricList.indexOf(name) < 0);
        metricSystem.report();
        metricSystem.registry().getGauges().values().forEach((gauge -> {
            if (gauge instanceof DataPointGauge) {
                ((DataPointGauge) gauge).reset();
            }
        }));
    }

    private static class DataPointGauge implements Gauge<Object> {
        private Object value;

        public DataPointGauge(Object initialValue) {
            this.setValue(initialValue);
        }

        @Override
        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public void reset() {
            this.value = 0;
        }
    }

    private static String buildUniqueTaskKey(TaskInfo taskInfo) {
        return String.format("%s[%s]", taskInfo.srcComponentId, taskInfo.srcTaskId);
    }

    private static String buildSimpleMetricName(TaskInfo taskInfo, String... name) {
        return String.join(".", StringUtils.join(name, ".").replace("/", "."));
    }

    @Override
    public void cleanup() {
        metricSystems.values().forEach(IMetricSystem::stop);
    }
}