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

package org.apache.eagle.security.auditlog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.builder.MetricDescriptor;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.security.util.LogParseUtil;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrafficParserBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficParserBolt.class);

    private static final String TARGET_METRIC_NAME = "hadoop.namenode.fsnamesystemstate.topuseropcounts";
    private static final String USER_METRIC_FORMAT = "hadoop.hdfs.auditlog.user.%sm.count";
    private static final String CLUSTER_METRIC_FORMAT = "hadoop.hdfs.auditlog.cluster.%sm.count";

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private OutputCollector collector;
    private Config config;
    private MetricDescriptor metricDescriptor;
    private ObjectMapper objectMapper;
    private IEagleServiceClient client;

    public TrafficParserBolt(Config config) {
        this.config = config;
        this.metricDescriptor = new MetricDescriptor();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.client = new EagleServiceClientImpl(config);
    }

    @Override
    public void execute(Tuple input) {
        Map event = null;
        try {
            event = StreamConvertHelper.tupleToEvent(input).f1();
            String resource = (String) event.get(metricDescriptor.getResourceField());
            if (resource.equalsIgnoreCase(TARGET_METRIC_NAME)) {
                String value = (String) event.get(metricDescriptor.getValueField());
                TopWindowResult rs = objectMapper.readValue(value, TopWindowResult.class);
                long tm = df.parse(rs.getTimestamp()).getTime() / DateTimeUtil.ONEMINUTE * DateTimeUtil.ONEMINUTE;

                for (TopWindowResult.TopWindow topWindow : rs.getWindows()) {
                    for (TopWindowResult.Op op : topWindow.getOps()) {
                        if (op.getOpType().equalsIgnoreCase("*")) {
                            generateMetric(op, topWindow.getWindowLenMs(), tm);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        } finally {
            collector.ack(input);
        }

    }

    private void generateMetric(TopWindowResult.Op op, int windowLen, long timestamp) {
        List<GenericMetricEntity> metrics = new ArrayList<>();
        GenericMetricEntity clusterMetric = buildMetricEntity(timestamp, CLUSTER_METRIC_FORMAT, null, op.getTotalCount(), windowLen);
        metrics.add(clusterMetric);
        collector.emit(new Values("", buildStreamEvent(clusterMetric)));
        for (TopWindowResult.User user : op.getTopUsers()) {
            GenericMetricEntity metric = buildMetricEntity(timestamp, USER_METRIC_FORMAT, user.getUser(), user.getCount(), windowLen);
            metrics.add(metric);
            collector.emit(new Values("", buildStreamEvent(metric)));
        }
        try {
            client.create(metrics);
            LOG.info("successfully create {} metrics", metrics.size());
        } catch (Exception e) {
            LOG.error("create {} metrics failed due to {}", metrics.size(), e.getMessage(), e);
        }
    }

    private GenericMetricEntity buildMetricEntity(long timestamp, String metricFormat, String user, long count, int windowLen) {
        GenericMetricEntity entity = new GenericMetricEntity();
        entity.setTimestamp(timestamp);
        entity.setValue(new double[]{Double.valueOf(count)});
        entity.setPrefix(String.format(metricFormat, windowLen / 60000));
        Map<String, String> tags = new HashMap<>();
        tags.put("site", config.getString("siteId"));
        tags.put("user", LogParseUtil.parseUserFromUGI(user));
        entity.setTags(tags);
        return entity;
    }

    private Map<String, Object> buildStreamEvent(GenericMetricEntity entity) {
        Map<String, Object> map = new HashMap<>();
        map.put("site", entity.getTags().get("site"));
        map.put("user", entity.getTags().get("user"));
        map.put("timestamp", entity.getTimestamp());
        map.put("metric", entity.getPrefix());
        map.put("value", entity.getValue()[0]);
        return map;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1", "f2"));
    }

    @Override
    public void cleanup() {
        if (client != null) {
            LOG.info("closing service client...");
            try {
                client.close();
            } catch (IOException e) {
                LOG.error("close service client failed due to {}", e.getMessage(), e);
            }
        }
    }
}
