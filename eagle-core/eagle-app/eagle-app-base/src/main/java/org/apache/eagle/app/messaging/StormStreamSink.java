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
package org.apache.eagle.app.messaging;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.apache.eagle.common.utils.Tuple2;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class StormStreamSink<K extends StreamSinkConfig> extends BaseRichBolt implements StreamSink<K> {
    private static final Logger LOG = LoggerFactory.getLogger(StormStreamSink.class);
    private String streamId;
    private OutputCollector collector;

    @Override
    public void init(String streamId, K config) {
        this.streamId = streamId;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Implicitly hides the Tuple protocol inside code as Tuple[Key,Map].
     */
    @Override
    public void execute(Tuple input) {
        try {
            Tuple2<Object,Map> keyValue = StreamConvertHelper.tupleToEvent(input);
            execute(keyValue.f0(), keyValue.f1(), collector);
            collector.ack(input);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        }
    }

    protected abstract void execute(Object key, Map event, OutputCollector collector) throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public String getStreamId() {
        return streamId;
    }
}