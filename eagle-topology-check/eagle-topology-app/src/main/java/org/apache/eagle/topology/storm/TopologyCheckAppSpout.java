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

package org.apache.eagle.topology.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Map;

public class TopologyCheckAppSpout extends BaseRichSpout {

    private TopologyDataExtractor extractor;
    private TopologyCheckAppConfig topologyCheckAppConfig;

    private long lastFetchTime;
    private long fetchInterval;

    private static final Logger LOG = LoggerFactory.getLogger(TopologyCheckAppSpout.class);

    public TopologyCheckAppSpout(TopologyCheckAppConfig topologyCheckAppConfig) {
        this.topologyCheckAppConfig = topologyCheckAppConfig;
        this.lastFetchTime = 0;
        this.fetchInterval = topologyCheckAppConfig.dataExtractorConfig.fetchDataIntervalInSecs * DateTimeUtil.ONESECOND;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.SERVICE_NAME_FIELD, TopologyConstants.TOPOLOGY_DATA_FIELD));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.extractor = new TopologyDataExtractor(topologyCheckAppConfig, collector);
    }

    @Override
    public void nextTuple() {
        long currentTime = System.currentTimeMillis();
        Calendar calendar = Calendar.getInstance();
        if (currentTime > lastFetchTime + fetchInterval) {
            calendar.setTimeInMillis(this.lastFetchTime);
            LOG.info("Last fetch time = {}", calendar.getTime());
            this.extractor.crawl();
            lastFetchTime = currentTime;
        }
    }

    @Override
    public void fail(Object msgId) {
        LOG.warn("ack {}", msgId.toString());
    }

    @Override
    public void ack(Object msgId) {
        LOG.info("ack {}", msgId.toString());
    }

}
