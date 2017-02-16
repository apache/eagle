/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Since 4/29/16.
 */
@SuppressWarnings( {"serial", "rawtypes", "unused"})
public class TestStormStreamIdRouting {
    @Ignore
    @Test
    public void testRoutingByStreamId() throws Exception {
        Config conf = new Config();
        conf.setNumWorkers(2); // use two worker processes
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("blue-spout", new BlueSpout()); // parallelism hint

        topologyBuilder.setBolt("green-bolt-1", new GreenBolt(1))
            .shuffleGrouping("blue-spout", "green-bolt-stream-1");
        topologyBuilder.setBolt("green-bolt-2", new GreenBolt(2))
            .shuffleGrouping("blue-spout", "green-bolt-stream-2");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mytopology", new HashMap(), topologyBuilder.createTopology());

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class BlueSpout extends BaseRichSpout {
        int count = 0;
        private SpoutOutputCollector collector;

        public BlueSpout() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("green-bolt-stream-1", new Fields("a"));
            declarer.declareStream("green-bolt-stream-2", new Fields("a"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            if (count % 2 == 0) {
                this.collector.emit("green-bolt-stream-1", Arrays.asList("testdata" + count));
                count++;
            } else {
                this.collector.emit("green-bolt-stream-2", Arrays.asList("testdata" + count));
                count++;
            }
            try {
                Thread.sleep(10000);
            } catch (Exception ex) {

            }
        }
    }

    private static class GreenBolt extends BaseRichBolt {
        private int id;

        public GreenBolt(int id) {
            this.id = id;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        }

        @Override
        public void execute(Tuple input) {
            System.out.println("bolt " + id + " received data " + input.getString(0));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a"));
        }
    }

    private static class YellowBolt extends BaseRichBolt {
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        }

        @Override
        public void execute(Tuple input) {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a"));
        }
    }
}
