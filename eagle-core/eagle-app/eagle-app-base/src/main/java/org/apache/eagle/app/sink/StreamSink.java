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
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.app.ApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class StreamSink extends BaseBasicBolt {
    private final static Logger LOG = LoggerFactory.getLogger(StreamSink.class);
    public final static String KEY_FIELD = "KEY";
    public final static String VALUE_FIELD = "VALUE";

    public StreamSink(StreamDefinition streamDefinition,ApplicationContext applicationContext){
        
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        List<Object> values = input.getValues();
        Object inputValue;
        if(values.size() == 1){
            inputValue = values.get(0);
        } else if(values.size() == 2){
            inputValue = values.get(1);
        } else{
            collector.reportError(new IllegalStateException("Expect tuple in size of 1: <StreamEvent> or 2: <Object,StreamEvent>, but got "+values.size()+": "+values));
            return;
        }

        if(inputValue instanceof StreamEvent){
            try {
                onEvent((StreamEvent) inputValue);
            }catch (Exception e){
                LOG.error("Failed to execute event {}",inputValue);
                collector.reportError(e);
            }
        } else {
            LOG.error("{} is not StreamEvent",inputValue);
            collector.reportError(new IllegalStateException("Input tuple "+input+"is not type of StreamEvent"));
        }
    }

    protected abstract void onEvent(StreamEvent streamEvent);

    public abstract Map<String,Object> getSinkContext();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KEY_FIELD,VALUE_FIELD));
    }
}