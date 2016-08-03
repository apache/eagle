/**
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
package org.apache.eagle.app.sink;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Preconditions;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class StormStreamSink<K extends StreamSinkConfig> extends BaseBasicBolt implements StreamSink<K> {
    private final static Logger LOG = LoggerFactory.getLogger(StormStreamSink.class);
    private final static String KEY_FIELD = "KEY";
    final static String VALUE_FIELD = "VALUE";
    private StreamEventMapper streamEventMapper = null;
    private String streamId;

    @Override
    public void init(String streamId, K config) {
        this.streamId = streamId;
        this.streamEventMapper = new FlattenEventMapper(streamId);
    }

    public StormStreamSink<K> setEventMapper(StreamEventMapper eventMapper){
        this.streamEventMapper = eventMapper;
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        Preconditions.checkNotNull(this.streamEventMapper);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            List<StreamEvent> streamEvents = this.streamEventMapper.map(input);
            if(streamEvents!=null) {
                streamEvents.forEach((streamEvent -> {
                    try {
                        onEvent(streamEvent);
                    } catch (Exception e) {
                        LOG.error("Failed to execute event {}", streamEvent);
                        collector.reportError(e);
                    }
                }));
            }
        } catch (Exception e) {
                LOG.error("Failed to execute event {}",input);
                collector.reportError(e);
        }
    }

    protected abstract void onEvent(StreamEvent streamEvent);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KEY_FIELD,VALUE_FIELD));
    }

    public String getStreamId() {
        return streamId;
    }
}