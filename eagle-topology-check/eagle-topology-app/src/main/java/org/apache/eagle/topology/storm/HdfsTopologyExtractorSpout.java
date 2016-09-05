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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.apache.eagle.topology.TopologyCheckAppConfig;

import java.util.Map;

public class HdfsTopologyExtractorSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyCheckAppConfig topologyCheckAppConfig;
    private long lastFetchTime;
    private long fetchInterval;

    public HdfsTopologyExtractorSpout(TopologyCheckAppConfig topologyCheckAppConfig) {
        this.topologyCheckAppConfig = topologyCheckAppConfig;
        this.lastFetchTime = 0;
        this.fetchInterval = topologyCheckAppConfig.dataExtractorConfig.fetchDataIntervalInSecs * DateTimeUtil.ONESECOND;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entities", "metrics"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        long currentTime = System.currentTimeMillis();
        if (currentTime < lastFetchTime + fetchInterval) {

        }
    }


}
