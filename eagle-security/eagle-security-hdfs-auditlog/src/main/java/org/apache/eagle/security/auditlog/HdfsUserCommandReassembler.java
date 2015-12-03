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
package org.apache.eagle.security.auditlog;

import com.typesafe.config.Config;
import org.apache.eagle.alert.siddhi.AttributeType;
import org.apache.eagle.alert.siddhi.SiddhiStreamMetadataUtils;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;

/**
 * Created by yonzhang on 11/20/15.
 */
public class HdfsUserCommandReassembler extends JavaStormStreamExecutor2<String, Map> {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsUserCommandReassembler.class);
    private Config config;
    private InputHandler inputHandler;
    /**
     * event schema is attribute name/type pairs
     */
    private final String streamName = "eventStream";
    public final static SortedMap<String, String> eventSchema = new TreeMap<String, String>(){{
        put("timestamp", AttributeType.LONG.name());
        put("src", AttributeType.STRING.name());
        put("dst", AttributeType.STRING.name());
        put("host", AttributeType.STRING.name());
        put("allowed", AttributeType.STRING.name());
        put("user", AttributeType.STRING.name());
        put("cmd", AttributeType.STRING.name());
    }};

    @Override
    public void prepareConfig(Config config) {
        this.config = config;
    }

    private static class GenericQueryCallback extends QueryCallback{
        private SortedMap<String, String> outputSelector;
        private SortedMap<String, String> outputModifier;
        public GenericQueryCallback(SortedMap<String, String> outputSelector, SortedMap<String, String> outputModifier){
            this.outputSelector = outputSelector;
            this.outputModifier = outputModifier;
        }
        @Override
        public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
            Object[] attrValues = inEvents[0].getData();
            Collector<Tuple2<String, Map>> collector = (Collector<Tuple2<String, Map>>)attrValues[0];
            SortedMap<String, Object> outputEvent = new TreeMap<String, Object>();
            int i = 1;  // output is from second element
            String user = null;
            for(String attrKey : outputSelector.keySet()){
                Object v = attrValues[i++];
                outputEvent.put(attrKey, v);
                if(attrKey.equals("user"))
                    user = (String)v;
            }

            outputEvent.putAll(outputModifier);
            LOG.debug("outputEvent: " + outputEvent);
            collector.collect(new Tuple2<String, Map>(user, outputEvent));
        }
    }

    @Override
    public void init() {
        String streamDef = SiddhiStreamMetadataUtils.convertToStreamDef(streamName, eventSchema);
        SiddhiManager siddhiManager = new SiddhiManager();
        StringBuilder sb = new StringBuilder();
        sb.append(streamDef);
        for(HdfsUserCommandPatternEnum rule : HdfsUserCommandPatternEnum.values()){
            sb.append(String.format("@info(name = '%s') from ", rule.getUserCommand()));
            sb.append(rule.getPattern());
            sb.append(" select a.context, ");
            for(SortedMap.Entry<String, String> entry : rule.getOutputSelector().entrySet()){
                sb.append(entry.getValue());
                sb.append(" as ");
                sb.append(entry.getKey());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append("insert into ");
            sb.append(rule.getUserCommand());
            sb.append("_outputStream;");
        }
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(sb.toString());

        for(HdfsUserCommandPatternEnum rule : HdfsUserCommandPatternEnum.values()){
            executionPlanRuntime.addCallback(rule.getUserCommand(), new GenericQueryCallback(rule.getOutputSelector(), rule.getOutputModifier()));
        }

        inputHandler = executionPlanRuntime.getInputHandler(streamName);
        executionPlanRuntime.start();
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, Map>> collector) {
        SortedMap<String, Object> toBeCopied = (SortedMap<String, Object>)input.get(1);
        SortedMap<String, Object> event = new TreeMap<String, Object>(toBeCopied);
        Object[] siddhiEvent = convertToSiddhiEvent(collector, event);
        try {
            inputHandler.send(siddhiEvent);
        }catch(Exception ex){
            LOG.error("fail sending event to Siddhi pattern engine", ex);
            throw new IllegalStateException(ex);
        }
    }

    public Object[] convertToSiddhiEvent(Object context, SortedMap<String, Object> event){
        Object[] siddhiEvent = new Object[1+event.size()];
        siddhiEvent[0] = context; // context
        int i = 1;
        for(Object value : event.values()){
            siddhiEvent[i++] = value;
        }
        return siddhiEvent;
    }
}
