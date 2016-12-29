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

package org.apache.eagle.security.traffic;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.typesafe.config.Config;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.utils.Tuple2;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.app.utils.ApplicationExecutionConfig.APP_ID_KEY;
import static org.apache.eagle.app.utils.ApplicationExecutionConfig.SITE_ID_KEY;

public class HadoopLogAccumulatorBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(HadoopLogAccumulatorBolt.class);

    private static final int DEFAULT_WINDOW_SIZE = 10;
    private static final String HADOOP_LOG_METRIC_NAME = "hadoop.log.count.minute";
    private static final String HDFS_COUNTER_WINDOW_SIZE = "dataSinkConfig.metricWindowSize";

    private int taskId;
    private String site;
    private String appId;
    private HadoopLogTrafficPersist client;
    private SimpleWindowCounter accumulator;
    private OutputCollector collector;
    private int windowSize;

    public HadoopLogAccumulatorBolt(Config config) {
        if (config.hasPath(SITE_ID_KEY)) {
            this.site = config.getString(SITE_ID_KEY);
        }
        if (config.hasPath(APP_ID_KEY)) {
            this.appId = config.getString(APP_ID_KEY);
        }
        if (config.hasPath(HDFS_COUNTER_WINDOW_SIZE)) {
            this.windowSize = config.getInt(HDFS_COUNTER_WINDOW_SIZE);
        } else {
            this.windowSize = DEFAULT_WINDOW_SIZE;
        }
        this.accumulator = new SimpleWindowCounter(windowSize);
        this.client = new HadoopLogTrafficPersist(config);

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.taskId = context.getThisTaskId();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Map<String, Object> toBeCopied = (Map<String, Object>) input.getValue(0);
        long timeInMs = (long) toBeCopied.get(HDFSAuditLogObject.HDFS_TIMESTAMP_KEY);
        long timeInMin = DateTimeUtil.roundDown(Calendar.MINUTE, timeInMs);
        try {
            collector.ack(input);
            if (!isOrdered(timeInMin)) {
                LOG.warn("data is out of order, the estimated throughput may be incorrect");
                return;
            }
            if (accumulator.isFull()) {
                Tuple2<Long, Long> pair = accumulator.poll();
                GenericMetricEntity metric = generateMetric(pair.f0(), pair.f1());
                client.emitMetric(metric);
            } else {
                accumulator.insert(timeInMin, 1);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    private boolean isOrdered(long timestamp) {
        if (accumulator.isEmpty() || !accumulator.isFull()) {
            return true;
        }
        return accumulator.peek() <= timestamp + windowSize * DateTimeUtil.ONEMINUTE;
    }

    private GenericMetricEntity generateMetric(long timestamp, long count) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        Map<String, String> tags = new HashMap<>();
        tags.put("appId", appId);
        tags.put("site", site);
        tags.put("taskId", String.valueOf(taskId));
        metricEntity.setTimestamp(timestamp);
        metricEntity.setTags(tags);
        metricEntity.setPrefix(HADOOP_LOG_METRIC_NAME);
        metricEntity.setValue(new double[] {count});
        return metricEntity;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
