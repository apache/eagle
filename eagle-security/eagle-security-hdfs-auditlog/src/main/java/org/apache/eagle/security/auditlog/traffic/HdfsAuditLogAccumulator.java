/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.auditlog.traffic;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.typesafe.config.Config;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.eagle.app.utils.ApplicationExecutionConfig.SITE_ID_KEY;

public class HdfsAuditLogAccumulator extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogAccumulator.class);
    private Map<Long, Long> accumulator = new ConcurrentHashMap<>(1);
    private static final long MAX_ACCUMULATOR_SIZE = 10;
    private static final String HDFS_AUDIT_LOG_METRIC_NAME = "hdfs.audit.log.count";
    private int taskId;
    private int taskIndex;
    private String site;
    private Config config;
    private String appType;
    private OutputCollector collector;

    public HdfsAuditLogAccumulator(Config config, String appType) {
        this.config = config;
        this.appType = appType;
        if (config.hasPath(SITE_ID_KEY)) {
            this.site = config.getString(SITE_ID_KEY);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        taskId = context.getThisTaskId();
        taskIndex = context.getThisTaskIndex();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Map<String, Object> toBeCopied = (Map<String, Object>) input.getValue(0);
        long timeInMs = (long) toBeCopied.get(HDFSAuditLogObject.HDFS_TIMESTAMP_KEY);
        long timeInMin = DateTimeUtil.roundDown(Calendar.MINUTE, timeInMs);
        try {
            if (accumulator.containsKey(timeInMin)) {
                accumulator.put(timeInMin, accumulator.get(timeInMin) + 1);
            } else {
                if (accumulator.size() < MAX_ACCUMULATOR_SIZE) {
                    accumulator.put(timeInMin, 1L);
                } else {
                    collector.emit(input, new Values(generateMetric(accumulator.get(timeInMin), timeInMin * DateTimeUtil.ONEMINUTE)));
                    accumulator.remove(timeInMin);
                }
            }
            collector.ack(input);
        } catch (Exception ex) {
            collector.fail(input);
            LOG.error(ex.getMessage(), ex);
        }
    }

    private GenericMetricEntity generateMetric(long count, long timestamp) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        Map<String, String> tags = new HashMap<>();
        tags.put("appType", appType);
        tags.put("site", site);
        tags.put("taskId", String.valueOf(taskId));
        metricEntity.setTimestamp(timestamp);
        metricEntity.setTags(tags);
        metricEntity.setPrefix(HDFS_AUDIT_LOG_METRIC_NAME);
        metricEntity.setValue(new double[] {count});
        return metricEntity;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }
}
