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

package org.apache.eagle.hadoop.queue.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.hadoop.queue.common.HadoopYarnResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HadoopQueueRunningSpout extends BaseRichSpout {

    private final static Logger LOG = LoggerFactory.getLogger(HadoopQueueRunningSpout.class);
    private final static String FETCH_INTERVAL_CONF = "dataSourceConfig.FetchIntervalSec";
    private final static String DEFAULT_FETCH_INTERVAL_SECONDS = "10";

    private long fetchIntervalSec;
    private long lastFetchTime = 0;

    private HadoopQueueRunningExtractor extractor;
    private Config config;

    public HadoopQueueRunningSpout(Config config) {
        this.config = config;
        fetchIntervalSec = Long.parseLong(HadoopYarnResourceUtils.getConfigValue(config, FETCH_INTERVAL_CONF, DEFAULT_FETCH_INTERVAL_SECONDS));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(HadoopClusterConstants.FIELD_DATATYPE, HadoopClusterConstants.FIELD_DATA));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        extractor = new HadoopQueueRunningExtractor(config, collector);
    }

    @Override
    public void nextTuple() {
        try {
            long fetchTime = System.currentTimeMillis();
            if (fetchTime > this.fetchIntervalSec * 1000 + this.lastFetchTime) {
                extractor.crawl();
                lastFetchTime = fetchTime;
            }
        } catch (Exception ex) {
            LOG.error("Fail crawling running queue resources and continue ...", ex);
        }
    }
}
