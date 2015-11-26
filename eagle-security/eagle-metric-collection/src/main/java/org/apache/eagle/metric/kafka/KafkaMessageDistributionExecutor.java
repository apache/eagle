/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.metric.kafka;

import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor1;
import org.apache.eagle.datastream.Tuple1;
import org.apache.eagle.metric.CountingMetric;
import org.apache.eagle.metric.Metric;
import org.apache.eagle.metric.manager.EagleMetricReportManager;
import org.apache.eagle.metric.report.EagleServiceMetricReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaMessageDistributionExecutor extends JavaStormStreamExecutor1<String> {

    private Config config;
    private Map<String, String> baseMetricDimension;
    private Map<String, EventMetric> eventMetrics;
    private static final long DEFAULT_METRIC_GRANULARITY = 5 * 60 * 1000;
    private static final String metricName = "kafka.message.user.count";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDistributionExecutor.class);

    public static class EventMetric {
        long latestMessageTime;
        Metric metric;

        public EventMetric(long latestMessageTime, Metric metric) {
            this.latestMessageTime = latestMessageTime;
            this.metric = metric;
        }

        public void update(double d) {
            this.metric.update(d);
        }
    }

    @Override
    public void prepareConfig(Config config) {
        this.config = config;
    }

    @Override
    public void init() {
        String site = config.getString("dataSourceConfig.site");
        String topic = config.getString("dataSourceConfig.topic");
        this.baseMetricDimension = new HashMap<>();
        this.baseMetricDimension.put("site", site);
        this.baseMetricDimension.put("topic", topic);
        String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        String username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
        String password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);

        EagleServiceMetricReport report = new EagleServiceMetricReport(eagleServiceHost, eagleServicePort, username, password);
        EagleMetricReportManager.getInstance().register("metricCollectServiceReport", report);
        eventMetrics = new ConcurrentHashMap<>();
    }

    public long trimTimestamp(long timestamp, long granularity) {
        return timestamp / granularity * granularity;
    }

    public void putNewMetric(long currentMessageTime, String user) {
        Map<String ,String> dimensions = new HashMap<>();
        dimensions.putAll(baseMetricDimension);
        dimensions.put("user", user);
        long trimTimestamp = trimTimestamp(currentMessageTime, DEFAULT_METRIC_GRANULARITY);
        Metric metric = new CountingMetric(trimTimestamp, dimensions, metricName, 1);
        eventMetrics.put(user, new EventMetric(currentMessageTime, metric));
    }

    public void update(long currentMessageTime, String user) {
        if (eventMetrics.get(user) == null) {
            LOG.info("A new user in the time interval, user: " + user + ", currentMessageTime: " + currentMessageTime);
            putNewMetric(currentMessageTime, user);
        }
        else {
            long latestMessageTime = eventMetrics.get(user).latestMessageTime;
            if (currentMessageTime > latestMessageTime + DEFAULT_METRIC_GRANULARITY) {
                LOG.info("Going to emit a user metric, user: " + user + ", currentMessageTime: " + currentMessageTime
                        + ", latestMessageTime: " + latestMessageTime);
                EagleMetricReportManager.getInstance().emit(Arrays.asList(eventMetrics.remove(user).metric));
                putNewMetric(currentMessageTime, user);
            }
            else {
                eventMetrics.get(user).update(1);
            }
        }
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple1<String>> collector) {
        try {
            String user = (String) input.get(0);
            Long timestamp = (Long) (input.get(1));
            update(timestamp, user);
        }
        catch (Exception ex) {
            LOG.error("Got an exception, ex: ", ex);
        }
    }
}
