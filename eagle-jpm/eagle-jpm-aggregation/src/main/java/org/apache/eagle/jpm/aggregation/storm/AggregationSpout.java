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
import org.apache.eagle.jpm.aggregation.state.AggregationTimeManager;
import org.apache.eagle.jpm.util.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AggregationSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationSpout.class);
    private static final Long MAX_WAIT_TIME = 12 * 60 * 60000L;//12 hours
    private static final Long MAX_SAFE_TIME = 6 * 60 * 60000L;//6 hours

    private Config config;
    MetricsAggregateContainer jobProcessTime;

    private SpoutOutputCollector collector;
    private Set<Long> processStartTime;
    private Long lastUpdateTime;

    public AggregationSpout(Config config, MetricsAggregateContainer jobProcessTime) {
        this.config = config;
        this.jobProcessTime = jobProcessTime;
        this.processStartTime = new HashSet<>();
    }

    @Override
    public void open(Map conf, TopologyContext context, final SpoutOutputCollector collector) {
        AggregationConfig.getInstance(config);
        AggregationTimeManager.instance().init(AggregationConfig.get().getZkStateConfig());
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(5);
        LOG.debug("start to run");
        try {
            //0, check processStartTime size
            if (this.processStartTime.size() > 0) {
                //bolts are processing
                return;
            }

            long currentJobTime = System.currentTimeMillis();//this.jobProcessTime.fetchLatestJobProcessTime();
            //1, get last updateTime;
            lastUpdateTime = AggregationTimeManager.instance().readLastFinishTime();
            if (lastUpdateTime == 0L) {
                //init state, just set to currentTime - 18 hours
                lastUpdateTime = (currentJobTime - (MAX_SAFE_TIME + MAX_WAIT_TIME)) / 3600000 * 3600000;
            }

            if (currentJobTime - lastUpdateTime < MAX_WAIT_TIME) {
                LOG.info("wait for max wait time, remaining {}ms", MAX_WAIT_TIME - currentJobTime + lastUpdateTime);
                return;
            }

            for (Long startTime = lastUpdateTime; startTime < lastUpdateTime + MAX_SAFE_TIME;) {
                collector.emit(new Values(startTime), startTime);
                this.processStartTime.add(startTime);
                startTime += AggregationConfig.get().getJobExtractorConfig().aggregationDuration * 1000;
            }
        } catch (Exception e) {
            LOG.warn("{}", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("startTime"));
    }

    @Override
    public void ack(Object messageId) {
        //remove from processStartTime, if size == 0, then update lastUpdateTime
        Long startTime = (Long)messageId;
        LOG.debug("succeed startTime {}", startTime);

        this.processStartTime.remove(startTime);
        if (this.processStartTime.size() == 0) {
            while (true) {
                try {
                    LOG.info("all have finished, update lastUpdateTime to {}", this.lastUpdateTime + MAX_SAFE_TIME);
                    AggregationTimeManager.instance().updateLastFinishTime(this.lastUpdateTime + MAX_SAFE_TIME);
                    break;
                } catch (Exception e) {
                    Utils.sleep(3);
                }
            }
        }
    }

    @Override
    public void fail(Object messageId) {
        Long startTime = (Long)messageId;
        LOG.warn("failed startTime {}", startTime);
        this.collector.emit(new Values(startTime), startTime);
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void close() {
    }
}
