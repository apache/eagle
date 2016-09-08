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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Since 4/29/16.
 */
@SuppressWarnings( {"rawtypes", "serial"})
public class TestStormCustomGroupingRouting implements Serializable {
    @Ignore
    @Test
    public void testRoutingByCustomGrouping() throws Exception {
        Config conf = new Config();
        conf.setNumWorkers(2); // use two worker processes
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("blue-spout", new BlueSpout()); // parallelism hint

        topologyBuilder.setBolt("green-bolt-1", new GreenBolt(0)).setNumTasks(2)
            .customGrouping("blue-spout", new CustomStreamGrouping() {
                int count = 0;
                List<Integer> targetTask;

                @Override
                public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
                    this.targetTask = targetTasks;
                }

                @Override
                public List<Integer> chooseTasks(int taskId, List<Object> values) {
                    if (count % 2 == 0) {
                        count++;
                        return Arrays.asList(targetTask.get(0));
                    } else {
                        count++;
                        return Arrays.asList(targetTask.get(1));
                    }
                }
            });

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
            declarer.declare(new Fields("a"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            if (count % 2 == 0) {
                this.collector.emit(Arrays.asList("testdata" + count));
                count++;
            } else {
                this.collector.emit(Arrays.asList("testdata" + count));
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
            System.out.flush();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a"));
        }
    }
}
