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

/**
 *
 */
package org.apache.eagle.hadoop.queue.crawler;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.MetricName;
import org.apache.eagle.hadoop.queue.model.applications.App;
import org.apache.eagle.hadoop.queue.model.applications.Apps;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueMessageId;
import org.apache.eagle.log.entity.GenericMetricEntity;
import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunningAppParseListener {

    private static final Logger logger = LoggerFactory.getLogger(RunningAppParseListener.class);
    private static final long AGGREGATE_INTERVAL = DateTimeUtil.ONEMINUTE;

    @SuppressWarnings("serial")
    public static HashMap<String, String> metrics = new HashMap<String, String>() {
        {
            put(MetricName.HADOOP_APPS_ALLOCATED_MB, "getAllocatedMB");
            put(MetricName.HADOOP_APPS_ALLOCATED_VCORES, "getAllocatedVCores");
            put(MetricName.HADOOP_APPS_RUNNING_CONTAINERS, "getRunningContainers");
        }
    };

    private String site;
    private SpoutOutputCollector collector;
    private Map<String, GenericMetricEntity> appMetricEntities = new HashMap<>();

    public RunningAppParseListener(String site, SpoutOutputCollector collector) {
        this.site = site;
        this.collector = collector;
    }

    public void flush() {
        logger.info("start sending app metrics, size: " + appMetricEntities.size());
        HadoopQueueMessageId messageId = new HadoopQueueMessageId(HadoopClusterConstants.DataType.METRIC, HadoopClusterConstants.DataSource.RUNNING_APPS, System.currentTimeMillis());
        List<GenericMetricEntity> metrics = new ArrayList<>(appMetricEntities.values());
        collector.emit(new ValuesArray(HadoopClusterConstants.DataType.METRIC.name(), metrics), messageId);
        appMetricEntities.clear();
    }

    private Map<String, String> buildMetricTags(AggLevel level, Map<String, String> tags) {
        Map<String, String> newTags = new HashMap<String, String>();
        newTags.put(HadoopClusterConstants.TAG_SITE, site);
        tags.entrySet().stream().filter(entry -> level.level.equalsIgnoreCase(entry.getKey())).forEach(entry -> {
            newTags.put(entry.getKey(), entry.getValue());
        });
        return newTags;
    }

    private void createMetric(String metricName, Map<String, String> tags, long timestamp, int value) {
        String key = metricName + tags.toString() + " " + timestamp;
        GenericMetricEntity entity = appMetricEntities.get(key);
        if (entity == null) {
            entity = new GenericMetricEntity();
            entity.setTags(tags);
            entity.setTimestamp(timestamp);
            entity.setPrefix(metricName);
            entity.setValue(new double[] {0.0});
            appMetricEntities.put(key, entity);
        }
        double lastValue = entity.getValue()[0];
        entity.setValue(new double[] {lastValue + value});
    }

    public void onMetric(Apps apps, long timestamp) throws Exception {
        timestamp = timestamp / AGGREGATE_INTERVAL * AGGREGATE_INTERVAL;
        for (App app : apps.getApp()) {
            Map<String, String> tags = new HashMap<>();
            tags.put(HadoopClusterConstants.TAG_USER, app.getUser());
            tags.put(HadoopClusterConstants.TAG_QUEUE, app.getQueue());
            for (AggLevel level : AggLevel.values()) {
                Map<String, String> newTags = buildMetricTags(level, tags);
                for (java.util.Map.Entry<String, String> entry : metrics.entrySet()) {
                    Method method = App.class.getMethod(entry.getValue());
                    Integer value = (Integer) method.invoke(app);
                    String metricName = String.format(entry.getKey(), level.name);
                    createMetric(metricName, newTags, timestamp, value);
                }
            }
        }
    }

    private enum AggLevel {
        CLUSTER(HadoopClusterConstants.TAG_CLUSTER, ""),
        QUEUE(HadoopClusterConstants.TAG_QUEUE, HadoopClusterConstants.TAG_QUEUE),
        USER(HadoopClusterConstants.TAG_USER, HadoopClusterConstants.TAG_USER);

        private String name;
        private String level;

        AggLevel(String name, String level) {
            this.name = name;
            this.level = level;
        }
    }
}
