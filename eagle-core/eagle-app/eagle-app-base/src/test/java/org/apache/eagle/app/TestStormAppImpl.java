/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.eagle.app.spi.AbstractApplicationProvider;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

//@Ignore
//public class TestStormAppImpl extends StormApp {
//    private final static Logger LOG = LoggerFactory.getLogger(TestStormAppImpl.class);
//    protected void buildApp(TopologyBuilder builder, ApplicationContainer context) {
//        builder.setSpout("metric_spout", new RandomEventSpout(), 4);
//        builder.setBolt("sink_1",context.getFlattenStreamSink("TEST_STREAM_1")).fieldsGrouping("metric_spout",new Fields("metric"));
//        builder.setBolt("sink_2",context.getFlattenStreamSink("TEST_STREAM_2")).fieldsGrouping("metric_spout",new Fields("metric"));
//    }
//
//    private class RandomEventSpout extends BaseRichSpout {
//        private SpoutOutputCollector _collector;
//        @Override
//        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
//            _collector = spoutOutputCollector;
//        }
//
//        @Override
//        public void nextTuple() {
//            _collector.emit(Arrays.asList("disk.usage",System.currentTimeMillis(),"host_1",56.7));
//            _collector.emit(Arrays.asList("cpu.usage",System.currentTimeMillis(),"host_2",99.8));
//        }
//
//        @Override
//        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//            outputFieldsDeclarer.declare(new Fields("metric","timestamp","source","value"));
//        }
//    }
//
//    public static class Provider extends AbstractApplicationProvider<TestStormAppImpl> {
//        public Provider(){
//            super("TestApplicationMetadata.xml");
//        }
//
//        @Override
//        public TestStormAppImpl getApplication() {
//            return new TestStormAppImpl();
//        }
//    }
//}