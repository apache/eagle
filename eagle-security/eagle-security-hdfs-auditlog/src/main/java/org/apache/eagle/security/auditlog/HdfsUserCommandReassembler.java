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
import org.apache.eagle.policy.siddhi.AttributeType;
import org.apache.eagle.policy.siddhi.SiddhiStreamMetadataUtils;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.entity.HdfsUserCommandPatternEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;

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
        private Map<String, String> outputSelector;
        private Map<String, String> outputModifier;
        public GenericQueryCallback(Map<String, String> outputSelector, Map<String, String> outputModifier){
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
        String readFrom = null;
        try {
            readFrom = config.getString("eagleProps.readHdfsUserCommandPatternFrom");
        }catch(Exception ex){
            LOG.warn("no config for readHdfsUserCommandPatternFrom", ex);
            readFrom = "file";
        }
        List<HdfsUserCommandPatternEntity> list = null;
        try {
            if (readFrom.equals("file")) {
                list = new HdfsUserCommandPatternByFileImpl().findAllPatterns();
            } else {
                list = new HdfsUserCommandPatternByDBImpl(new EagleServiceConnector(config)).findAllPatterns();
            }
        }catch(Exception ex){
            LOG.error("fail reading hfdsUserCommandPattern", ex);
            throw new IllegalStateException(ex);
        }
        for(HdfsUserCommandPatternEntity rule : list){
            sb.append(String.format("@info(name = '%s') from ", rule.getTags().get("userCommand")));
            sb.append(rule.getPattern());
            sb.append(" select a.context, ");
            for(Map.Entry<String, String> entry : rule.getFieldSelector().entrySet()){
                sb.append(entry.getValue());
                sb.append(" as ");
                sb.append(entry.getKey());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append("insert into ");
            sb.append(rule.getTags().get("userCommand"));
            sb.append("_outputStream;");
        }

        LOG.info("patterns: " + sb.toString());
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(sb.toString());

        for(HdfsUserCommandPatternEntity rule : list){
            executionPlanRuntime.addCallback(rule.getTags().get("userCommand"), new GenericQueryCallback(rule.getFieldSelector(), rule.getFieldModifier()));
        }

        inputHandler = executionPlanRuntime.getInputHandler(streamName);
        executionPlanRuntime.start();
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, Map>> collector) {
        if(LOG.isDebugEnabled()) LOG.debug("incoming event:" + input.get(1));
        SortedMap<String, Object> toBeCopied = (SortedMap<String, Object>) input.get(1);
        SortedMap<String, Object> event = new TreeMap<>(toBeCopied);
        Object[] siddhiEvent = convertToSiddhiEvent(collector, event);
        try {
            inputHandler.send(siddhiEvent);
        } catch (Exception ex){
            LOG.error("Fail sending event to Siddhi pattern engine", ex);
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