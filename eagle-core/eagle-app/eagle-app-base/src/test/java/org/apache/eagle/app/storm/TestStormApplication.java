/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.storm;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import org.apache.eagle.app.ApplicationConfig;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.StormEnvironment;

import java.util.Arrays;
import java.util.Map;

public class TestStormApplication extends StormApplication<TestStormApplication.TestStormApplicationConfig> {
    @Override
    public StormTopology execute(TestStormApplicationConfig config, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("metric_spout", new RandomEventSpout(), config.getSpoutNum());
        return builder.createTopology();
    }

    public static class TestStormApplicationConfig implements ApplicationConfig {
        private int spoutNum;

        @Override
        public void load(Config config) {
            this.spoutNum = config.getInt("application.spout.num");
        }

        public int getSpoutNum() {
            return spoutNum;
        }

        public void setSpoutNum(int spoutNum) {
            this.spoutNum = spoutNum;
        }
    }

    private class RandomEventSpout extends BaseRichSpout {
        private SpoutOutputCollector _collector;
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            _collector.emit(Arrays.asList("disk.usage",System.currentTimeMillis(),"host_1",56.7));
            _collector.emit(Arrays.asList("cpu.usage",System.currentTimeMillis(),"host_2",99.8));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("metric","timestamp","source","value"));
        }
    }
}