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
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.hadoop.jmx.model.HdfsServiceTopologyAPIEntity;
import org.apache.eagle.hadoop.jmx.model.TopologyResult;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.*;

public class HdfsTopologyParserBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTopologyParserBolt.class);

    private HdfsTopologyParser parser;
    private HadoopTopologyUpdater updater;
    private OutputCollector collector;
    private HadoopJmxMonitorConfig config;

    public HdfsTopologyParserBolt(HadoopJmxMonitorConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.parser = new HdfsTopologyParser(config.site);
        this.updater = new HadoopTopologyUpdater(config);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            Map<String, JMXBean> jmxBeanMap = (Map<String, JMXBean>) input.getValue(0);
            TopologyResult result = parser.parse(jmxBeanMap);
            if (result == null || result.getEntities().isEmpty() || result.getMetrics().isEmpty()) {
                return;
            }
            for (TaggedLogAPIEntity entity : result.getEntities()) {
                HdfsServiceTopologyAPIEntity hdfsServiceTopologyAPIEntity = (HdfsServiceTopologyAPIEntity) entity;
                collector.emit(new Values(PLACE_HOLDER, streamTopologyEntity(hdfsServiceTopologyAPIEntity)));
            }
            updater.update(HDFS_INSTANCE_SERVICE_NAME, result);
        } catch (Exception e) {
            LOG.error("Catch a fetal error with message {}", e.getMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1", "f2"));
    }

    private Map<String, Object> streamTopologyEntity(HdfsServiceTopologyAPIEntity entity) {
        Map<String, Object> map = new HashMap<>();
        map.put("status", entity.getStatus());
        map.put("timestamp", entity.getTimestamp());
        map.put("host", entity.getTags().get(HOSTNAME_TAG));
        map.put("role", entity.getTags().get(ROLE_TAG));
        map.put("site", config.site);
        return map;
    }


}
