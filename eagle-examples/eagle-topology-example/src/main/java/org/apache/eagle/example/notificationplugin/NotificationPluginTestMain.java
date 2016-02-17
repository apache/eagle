/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.example.notificationplugin;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created on 2/16/16.
 */
public class NotificationPluginTestMain {
    public static void main(String[] args){
        System.setProperty("config.resource", "/application-plugintest.conf");
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm();
        env.fromSpout(createProvider(env.getConfig())).withOutputFields(2).nameAs("testSpout").alertWithConsumer("testStream", "testExecutor");
        env.execute();
    }

    public static StormSpoutProvider createProvider(Config config) {
        return new StormSpoutProvider(){

            @Override
            public BaseRichSpout getSpout(Config context) {
                return new TestSpout();
            }
        };
    }

    public static class TestSpout extends BaseRichSpout {
        private static final Logger LOG = LoggerFactory.getLogger(TestSpout.class);
        private SpoutOutputCollector collector;
        public TestSpout() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(5000);
            LOG.info("emitted tuple ...");
            Map<String, Object> map = new TreeMap<>();
            map.put("testAttribute", "testValue");
            collector.emit(new Values("testStream", map));
        }
    }
}
