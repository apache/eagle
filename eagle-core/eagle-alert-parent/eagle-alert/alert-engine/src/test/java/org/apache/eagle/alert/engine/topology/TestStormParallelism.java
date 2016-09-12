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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Since 4/29/16.
 */
@SuppressWarnings( {"serial", "rawtypes"})
public class TestStormParallelism {
    /**
     * When run this test, please check the following through jstack and log
     * 1) for blue-spout, num of executors is 2, # of tasks is 2
     * <p>
     * Expected:
     * <p>
     * a. 2 threads uniquely named Thread-*-blue-spout-executor[*,*]
     * b. each thread will have single task
     * <p>
     * 2) for green-bolt, num of executors is 2, # of tasks is 4
     * <p>
     * Expected:
     * <p>
     * a. 2 threads uniquely named Thread-*-green-bolt-executor[*,*]
     * b. each thread will have 2 tasks
     * <p>
     * 3) for yellow-bolt, num of executors is 6, # of tasks is 6
     * <p>
     * Expected:
     * <p>
     * a. 6 threads uniquely named Thread-*-yellow-bolt-executor[*,*]
     * b. each thread will have 1 tasks
     * <p>
     * <p>
     * Continue to think:
     * <p>
     * For alter engine, if we use multiple tasks per component instead of one task per component,
     * what will the parallelism mechanism affect?
     *
     * @throws Exception
     */
    @Ignore
    @Test
    public void testParallelism() throws Exception {
        Config conf = new Config();
        conf.setNumWorkers(2); // use two worker processes
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("blue-spout", new BlueSpout(), 2); // parallelism hint

        topologyBuilder.setBolt("green-bolt", new GreenBolt(), 2)
            .setNumTasks(4)
            .shuffleGrouping("blue-spout");

        topologyBuilder.setBolt("yellow-bolt", new YellowBolt(), 6)
            .shuffleGrouping("green-bolt");

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
        static int count = 0;

        public BlueSpout() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            count++;
            System.out.println("# of spout objects " + count + ", current spout " + this);
        }

        @Override
        public void nextTuple() {

        }
    }

    private static class GreenBolt extends BaseRichBolt {
        static int count;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            count++;
            System.out.println("# of green bolt objects " + count + ", current green bolt " + this);
        }

        @Override
        public void execute(Tuple input) {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a"));
        }
    }

    private static class YellowBolt extends BaseRichBolt {
        static int count;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            count++;
            System.out.println("# of yellow bolt objects " + count + ", current yellow bolt " + this);
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
