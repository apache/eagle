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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.metric.reportor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaMessageDistributionBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageDistributionBolt.class);
    private static final long DEFAULT_METRIC_GRANULARITY = 60 * 1000;
    private Config config;
    private Map<String, String> baseMetricDimension;
    private MetricRegistry registry;
    private EagleMetricListener listener;
    private long granularity;
    private OutputCollector collector;

    public KafkaMessageDistributionBolt(Config config){
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String site = config.getString("dataSourceConfig.site");
        String topic = config.getString("dataSourceConfig.topic");
        this.baseMetricDimension = new HashMap<>();
        this.baseMetricDimension.put("site", site);
        this.baseMetricDimension.put("topic", topic);
        registry = new MetricRegistry();

        this.granularity = DEFAULT_METRIC_GRANULARITY;
        if (config.hasPath("dataSourceConfig.kafkaDistributionDataIntervalMin")) {
            this.granularity = config.getInt("dataSourceConfig.kafkaDistributionDataIntervalMin") * DateUtils.MILLIS_PER_MINUTE;
        }

        String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        int port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        String username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
        String password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);
        listener = new EagleServiceReporterMetricListener(host, port, username, password);
    }

    public String generateMetricKey(String user) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.putAll(baseMetricDimension);
        dimensions.put("user", user);
        String metricName = "eagle.kafka.message.count";
        String encodedMetricName = MetricKeyCodeDecoder.codeMetricKey(metricName, dimensions);
        return encodedMetricName;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input) {
        try {
            String user = input.getString(0);
            Long timestamp = input.getLong(1);
            String metricKey = generateMetricKey(user);
            if (registry.getMetrics().get(metricKey) == null) {
                EagleCounterMetric metric = new EagleCounterMetric(timestamp, metricKey, 1.0, granularity);
                metric.registerListener(listener);
                registry.register(metricKey, metric);
            }
            else {
                EagleMetric metric = (EagleMetric)registry.getMetrics().get(metricKey);
                metric.update(1, timestamp);
                //TODO: if we need to remove metric from registry
            }
        }
        catch (Exception ex) {
            LOG.error("Got an exception, ex: ", ex);
        }finally {
            collector.ack(input);
        }
    }
}
