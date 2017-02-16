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
package org.apache.eagle.app.environment.builder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransformFunctionBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TransformFunctionBolt.class);
    private final TransformFunction function;
    private OutputCollector collector;

    public TransformFunctionBolt(TransformFunction function) {
        this.function = function;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.function.open(new StormOutputCollector(collector));
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            this.function.transform(StreamConvertHelper.tupleToEvent(input).f1());
            this.collector.ack(input);
        } catch (Throwable throwable) {
            LOG.error("Transform error: {}", input, throwable);
            this.collector.reportError(throwable);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1","f2"));
    }

    @Override
    public void cleanup() {
        this.function.close();
    }
}