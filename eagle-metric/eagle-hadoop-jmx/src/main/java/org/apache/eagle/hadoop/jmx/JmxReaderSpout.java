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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.eagle.app.utils.connection.URLResourceFetcher;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class JmxReaderSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(JmxReaderSpout.class);
    private HadoopJmxMonitorConfig config;
    private String component;
    private String downStreamId;
    private String metricStreamId;
    private HadoopHAStateChecker haChecker;

    public JmxReaderSpout(HadoopJmxMonitorConfig config, String component, String downStreamId, String metricStreamId) {
        this.config = config;
        this.component = component;
        this.downStreamId = downStreamId;
        this.metricStreamId = metricStreamId;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.haChecker = new NamenodeHAStateChecker(config.site, component, collector);
    }

    @Override
    public void nextTuple() {
        try {
            haChecker.checkAndEmit(config.dataSourceConfig.namenodeUrls, downStreamId, metricStreamId);
        } catch (Exception e) {
            LOG.error("Catch a fetal error with message {}", e.getMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(downStreamId, new Fields("f1"));
        declarer.declareStream(metricStreamId, new Fields("f1", "f2"));
    }

}
