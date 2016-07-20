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
import backtype.storm.tuple.Values;
import org.apache.eagle.app.spi.AbstractApplicationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TestApplicationImpl extends AbstractApplication {
    private final static Logger LOG = LoggerFactory.getLogger(TestApplicationImpl.class);
    public class RandomEventSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;

        @SuppressWarnings("rawtypes")
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {

        }

        @Override
        public void ack(Object id) {
            //Ignored
        }

        @Override
        public void fail(Object id) {
            _collector.emit(new Values(id), id);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key","event"));
        }
    }

    protected void buildTopology(TopologyBuilder builder, ApplicationContext context) {
        builder.setSpout("mockMetricSpout", new RandomEventSpout(), 4);
        builder.setBolt("sink_1",context.getStreamSink("TEST_STREAM_1")).fieldsGrouping("mockMetricSpout",new Fields("key"));
        builder.setBolt("sink_2",context.getStreamSink("TEST_STREAM_2")).fieldsGrouping("mockMetricSpout",new Fields("key"));
    }

    public static class Provider extends AbstractApplicationProvider<TestApplicationImpl> {
        public Provider(){
            super("TestApplicationMetadata.xml");
        }

        @Override
        public TestApplicationImpl getApplication() {
            return new TestApplicationImpl();
        }
    }
}