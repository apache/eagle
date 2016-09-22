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
package org.apache.eagle.alert.engine.evaluator.impl;

import java.io.Serializable;
import java.util.Map;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinitionNotFoundException;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SiddhiPolicyHandler implements PolicyStreamHandler, Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(SiddhiPolicyHandler.class);
    private transient ExecutionPlanRuntime executionRuntime;
    private transient SiddhiManager siddhiManager;
    private Map<String, StreamDefinition> sds;
    private PolicyDefinition policy;
    private transient PolicyHandlerContext context;

    public SiddhiPolicyHandler(Map<String, StreamDefinition> sds){
        this.sds = sds;
    }

    private static String generateExecutionPlan(PolicyDefinition policyDefinition, Map<String, StreamDefinition> sds) throws StreamDefinitionNotFoundException {
        StringBuilder builder = new StringBuilder();
        for(String inputStream:policyDefinition.getInputStreams()) {
            builder.append(SiddhiDefinitionAdapter.buildStreamDefinition(sds.get(inputStream)));
            builder.append("\n");
        }
        builder.append(policyDefinition.getDefinition().value);
        if(LOG.isDebugEnabled()) LOG.debug("Generated siddhi execution plan: {} from policy: {}", builder.toString(),policyDefinition);
        return builder.toString();
    }

    private static class AlertStreamCallback extends StreamCallback{
        private final String outputStream;
        private final Collector<AlertStreamEvent> collector;
        private final PolicyHandlerContext context;
        private final StreamDefinition definition;

        public AlertStreamCallback(String outputStream, StreamDefinition streamDefinition, Collector<AlertStreamEvent> collector, PolicyHandlerContext context){
            this.outputStream = outputStream;
            this.collector = collector;
            this.context = context;
            this.definition = streamDefinition;
        }

        /**
         * Possibly more than one event will be triggered for alerting
         * @param events
         */
        @Override
        public void receive(Event[] events) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Generated {} alerts from policy '{}' in {}", events.length,context.getPolicyDefinition().getName(), context.getPolicyEvaluatorId());
            }
            for(Event e : events) {
                AlertStreamEvent event = new AlertStreamEvent();
                event.setTimestamp(e.getTimestamp());
                event.setData(e.getData());
                event.setStreamId(outputStream);
                event.setPolicy(context.getPolicyDefinition());
                if (this.context.getPolicyEvaluator() != null) {
                    event.setCreatedBy(context.getPolicyEvaluator().getName());
                }
                event.setCreatedTime(System.currentTimeMillis());
                event.setSchema(definition);
                if(LOG.isDebugEnabled())
                    LOG.debug("Generate new alert event: {}", event);
                collector.emit(event);
            }
            context.getPolicyCounter().scope(String.format("%s.%s",this.context.getPolicyDefinition().getName(),"alert_count")).incrBy(events.length);
        }
    }

    @Override
    public void prepare(final Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
        LOG.info("Initializing handler for policy {}: {}",context.getPolicyDefinition(),this);
        this.policy = context.getPolicyDefinition();
        this.siddhiManager = new SiddhiManager();
        String plan = generateExecutionPlan(policy, sds);
        try {
            this.executionRuntime = siddhiManager.createExecutionPlanRuntime(plan);
            LOG.info("Created siddhi runtime {}",executionRuntime.getName());
        }catch (Exception parserException){
            LOG.error("Failed to create siddhi runtime for policy: {}, siddhi plan: \n\n{}\n",context.getPolicyDefinition().getName(),plan,parserException);
            throw parserException;
        }
        for(final String outputStream:policy.getOutputStreams()){
            if(executionRuntime.getStreamDefinitionMap().containsKey(outputStream)) {
                this.executionRuntime.addCallback(outputStream,
                        new AlertStreamCallback(
                        outputStream, SiddhiDefinitionAdapter.convertFromSiddiDefinition(executionRuntime.getStreamDefinitionMap().get(outputStream))
                        ,collector, context));
            } else {
                throw new IllegalStateException("Undefined output stream "+outputStream);
            }
        }
        this.executionRuntime.start();
        this.context = context;
        LOG.info("Initialized policy handler for policy: {}",policy.getName());
    }

    public void send(StreamEvent event) throws Exception {
        context.getPolicyCounter().scope(String.format("%s.%s",this.context.getPolicyDefinition().getName(),"receive_count")).incr();
        String streamId = event.getStreamId();
        InputHandler inputHandler = executionRuntime.getInputHandler(streamId);
        if(inputHandler != null){
            context.getPolicyCounter().scope(String.format("%s.%s",this.context.getPolicyDefinition().getName(),"eval_count")).incr();
            inputHandler.send(event.getTimestamp(),event.getData());
            
            if (LOG.isDebugEnabled()) {
                LOG.debug("sent event to siddhi stream {} ", streamId);
            }
        }else{
            context.getPolicyCounter().scope(String.format("%s.%s",this.context.getPolicyDefinition().getName(),"drop_count")).incr();
            LOG.warn("No input handler found for stream {}",streamId);
        }
    }

    public void close() {
        LOG.info("Closing handler for policy {}",this.policy.getName());
        this.executionRuntime.shutdown();
        LOG.info("Shutdown siddhi runtime {}",this.executionRuntime.getName());
        this.siddhiManager.shutdown();
        LOG.info("Shutdown siddhi manager {}",this.siddhiManager);
        LOG.info("Closed handler for policy {}",this.policy.getName());
    }
}
