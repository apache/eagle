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

package org.apache.eagle.gc.executor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.common.config.EagleConfigHelper;
import org.apache.eagle.datastream.*;
import org.apache.eagle.gc.common.GCConstants;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.metric.reportor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class GCMetricGeneratorBolt extends BaseRichBolt {

    public final static Logger LOG = LoggerFactory.getLogger(GCMetricGeneratorBolt.class);
    private Config config;
    private MetricRegistry registry;
    private String gcPausedTimeMetricName;
    private String youngHeapUsageMetricName;
    private String tenuredHeapUsageMetricName;
    private String totalHeapUsageMetricName;
    private Map<String, String> dimensions;
    private List<EagleMetric> metrics = new ArrayList<>();

    private EagleServiceReporterMetricListener listener;
    private OutputCollector collector;

    public GCMetricGeneratorBolt(Config config){
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        registry = new MetricRegistry();
        String host = EagleConfigHelper.getServiceHost(config);
        int port = EagleConfigHelper.getServicePort(config);
        String username = EagleConfigHelper.getServiceUser(config);
        String password = EagleConfigHelper.getServicePassword(config);
        listener = new EagleServiceReporterMetricListener(host, port, username, password);
        dimensions = new HashMap<>();
        dimensions.put(EagleConfigConstants.SITE, config.getString("siteId"));
        dimensions.put(EagleConfigConstants.APPLICATION, config.getString("appId"));
        gcPausedTimeMetricName = MetricKeyCodeDecoder.codeMetricKey(GCConstants.GC_PAUSE_TIME_METRIC_NAME, dimensions);

        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1", "f2"));
    }
    public void registerMetricIfMissing(String metricName, EagleMetric metric) {
        if (registry.getMetrics().get(metricName) == null) {
            metric.registerListener(listener);
            registry.register(metricName, metric);
        }
    }
    @Override
    public void execute(Tuple input) {
        try {
            Map<String, Object> map = (Map<String, Object>) input.getValue(1);
            GCPausedEvent event = new GCPausedEvent(map);
            // Generate gc paused time metric
            EagleCounterMetric metric = new EagleCounterMetric(event.getTimestamp(), gcPausedTimeMetricName, event.getPausedGCTimeSec(), GCConstants.GC_PAUSE_TIME_METRIC_GRANULARITY);
            registerMetricIfMissing(gcPausedTimeMetricName, metric);

            // Generate young heap paused time metric
            if (event.isYoungAreaGCed()) {
                youngHeapUsageMetricName = MetricKeyCodeDecoder.codeTSMetricKey(event.getTimestamp(), GCConstants.GC_YOUNG_MEMORY_METRIC_NAME, dimensions);
                EagleGaugeMetric metric2 = new EagleGaugeMetric(event.getTimestamp(), youngHeapUsageMetricName, event.getYoungUsedHeapK());
                metrics.add(metric2);
            }

            // Generate tenured heap paused time metric
            if (event.isTenuredAreaGCed()) {
                tenuredHeapUsageMetricName = MetricKeyCodeDecoder.codeTSMetricKey(event.getTimestamp(), GCConstants.GC_TENURED_MEMORY_METRIC_NAME, dimensions);
                EagleGaugeMetric metric3 = new EagleGaugeMetric(event.getTimestamp(), tenuredHeapUsageMetricName, event.getTenuredUsedHeapK());
                metrics.add(metric3);
            }

            // Generate total heap paused time metric
            if (event.isTotalHeapUsageAvailable()) {
                totalHeapUsageMetricName = MetricKeyCodeDecoder.codeTSMetricKey(event.getTimestamp(), GCConstants.GC_TOTAL_MEMORY_METRIC_NAME, dimensions);
                EagleGaugeMetric metric4 = new EagleGaugeMetric(event.getTimestamp(), totalHeapUsageMetricName, event.getUsedTotalHeapK());
                metrics.add(metric4);
            }
            listener.onMetricFlushed(metrics);
            metrics.clear();
            collector.emit(Arrays.asList(input.getValue(0), input.getValue(1)));
        }catch(Exception ex){
            LOG.error("error in gc metric generating", ex);
        }finally {
            collector.ack(input);
        }
    }
}