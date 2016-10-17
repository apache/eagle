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

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamNotDefinedException;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.List;
import java.util.Map;

public class SiddhiPolicyHandler implements PolicyStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPolicyHandler.class);
    private ExecutionPlanRuntime executionRuntime;
    private SiddhiManager siddhiManager;
    private Map<String, StreamDefinition> sds;
    private PolicyDefinition policy;
    private PolicyHandlerContext context;

    private int currentIndex = 0; // the index of current definition statement inside the policy definition

    public SiddhiPolicyHandler(Map<String, StreamDefinition> sds, int index) {
        this.sds = sds;
        this.currentIndex = index;
    }

    protected String generateExecutionPlan(PolicyDefinition policyDefinition, Map<String, StreamDefinition> sds) throws StreamNotDefinedException {
        return SiddhiDefinitionAdapter.buildSiddhiExecutionPlan(policyDefinition,sds);
    }

    @Override
    public void prepare(final Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
        LOG.info("Initializing handler for policy {}", context.getPolicyDefinition());
        this.policy = context.getPolicyDefinition();
        this.siddhiManager = new SiddhiManager();
        String plan = generateExecutionPlan(policy, sds);
        try {
            this.executionRuntime = siddhiManager.createExecutionPlanRuntime(plan);
            LOG.info("Created siddhi runtime {}", executionRuntime.getName());
        } catch (Exception parserException) {
            LOG.error("Failed to create siddhi runtime for policy: {}, siddhi plan: \n\n{}\n", context.getPolicyDefinition().getName(), plan, parserException);
            throw parserException;
        }

        // add output stream callback
        List<String> outputStreams = getOutputStreams(policy);
        for (final String outputStream : outputStreams) {
            if (executionRuntime.getStreamDefinitionMap().containsKey(outputStream)) {
                this.executionRuntime.addCallback(outputStream,
                    new AlertStreamCallback(
                        outputStream, SiddhiDefinitionAdapter.convertFromSiddiDefinition(executionRuntime.getStreamDefinitionMap().get(outputStream)),
                        collector, context, currentIndex));
            } else {
                throw new IllegalStateException("Undefined output stream " + outputStream);
            }
        }
        this.executionRuntime.start();
        this.context = context;
        LOG.info("Initialized policy handler for policy: {}", policy.getName());
    }

    protected List<String> getOutputStreams(PolicyDefinition policy) {
        return policy.getOutputStreams().isEmpty() ? policy.getDefinition().getOutputStreams() : policy.getOutputStreams();
    }

    public void send(StreamEvent event) throws Exception {
        context.getPolicyCounter().scope(String.format("%s.%s", this.context.getPolicyDefinition().getName(), "receive_count")).incr();
        String streamId = event.getStreamId();
        InputHandler inputHandler = executionRuntime.getInputHandler(streamId);
        if (inputHandler != null) {
            context.getPolicyCounter().scope(String.format("%s.%s", this.context.getPolicyDefinition().getName(), "eval_count")).incr();
            inputHandler.send(event.getTimestamp(), event.getData());

            if (LOG.isDebugEnabled()) {
                LOG.debug("sent event to siddhi stream {} ", streamId);
            }
        } else {
            context.getPolicyCounter().scope(String.format("%s.%s", this.context.getPolicyDefinition().getName(), "drop_count")).incr();
            LOG.warn("No input handler found for stream {}", streamId);
        }
    }

    public void close() throws Exception {
        LOG.info("Closing handler for policy {}", this.policy.getName());
        this.executionRuntime.shutdown();
        LOG.info("Shutdown siddhi runtime {}", this.executionRuntime.getName());
        this.siddhiManager.shutdown();
        LOG.info("Shutdown siddhi manager {}", this.siddhiManager);
        LOG.info("Closed handler for policy {}", this.policy.getName());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SiddhiPolicyHandler for policy: ");
        sb.append(this.policy == null ? "" : this.policy.getName());
        return sb.toString();
    }

}
