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

import org.apache.eagle.hadoop.queue.HadoopQueueRunningAppConfig;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HadoopQueueRunningSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopQueueRunningSpout.class);

    private long fetchIntervalSec;
    private long lastFetchTime = 0;

    private HadoopQueueRunningExtractor extractor;
    private HadoopQueueRunningAppConfig config;

    public HadoopQueueRunningSpout(HadoopQueueRunningAppConfig config) {
        this.config = config;
        fetchIntervalSec = Long.parseLong(config.dataSourceConfig.fetchIntervalSec);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(HadoopClusterConstants.FIELD_DATASOURCE,
                HadoopClusterConstants.FIELD_DATATYPE,
                HadoopClusterConstants.FIELD_DATA));
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
