/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.aggregation.storm;

import org.apache.eagle.jpm.aggregation.AggregationConfig;
import org.apache.eagle.jpm.aggregation.common.MetricsAggregateContainer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AggregationBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationBolt.class);
    private Config config;
    private OutputCollector collector;
    private MetricsAggregateContainer metricsAggregateContainer;

    public AggregationBolt(Config config, MetricsAggregateContainer metricsAggregateContainer) {
        this.config = config;
        this.metricsAggregateContainer = metricsAggregateContainer;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        AggregationConfig.getInstance(config);
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Long startTime = tuple.getLongByField("startTime");
        LOG.info("get startTime {}", startTime);
        Long endTime = startTime + AggregationConfig.get().getJobExtractorConfig().aggregationDuration * 1000;

        if (metricsAggregateContainer.aggregate(startTime, endTime)) {
            collector.ack(tuple);
            LOG.info("succeed startTime {}", startTime);
        } else {
            collector.fail(tuple);
            LOG.warn("failed startTime {}", startTime);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
