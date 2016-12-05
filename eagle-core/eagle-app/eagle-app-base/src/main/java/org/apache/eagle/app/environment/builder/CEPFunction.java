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

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter.convertFromSiddiDefinition;

public class CEPFunction implements TransformFunction {

    private static final Logger LOG = LoggerFactory.getLogger(CEPFunction.class);

    private ExecutionPlanRuntime runtime;
    private SiddhiManager siddhiManager;
    private final CEPDefinition cepDefinition;
    private Collector collector;

    public CEPFunction(CEPDefinition cepDefinition) {
        this.cepDefinition = cepDefinition;
    }

    public CEPFunction(String siddhiQuery, String inputStreamId, String outputStreamId) {
        this.cepDefinition = new CEPDefinition(siddhiQuery,inputStreamId, outputStreamId);
    }

    @Override
    public String getName() {
        return "CEPFunction";
    }

    @Override
    public void open(Collector collector) {
        this.collector = collector;
        this.siddhiManager = new SiddhiManager();
        this.runtime = siddhiManager.createExecutionPlanRuntime(cepDefinition.getSiddhiQuery());
        if (runtime.getStreamDefinitionMap().containsKey(cepDefinition.outputStreamId)) {
            runtime.addCallback(cepDefinition.outputStreamId, new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event e : events) {
                        StreamDefinition schema = convertFromSiddiDefinition(runtime.getStreamDefinitionMap().get(cepDefinition.outputStreamId));
                        Map<String, Object> event = new HashMap<>();
                        for (StreamColumn column : schema.getColumns()) {
                            Object obj = e.getData()[schema.getColumnIndex(column.getName())];
                            if (obj == null) {
                                event.put(column.getName(), null);
                                continue;
                            }
                            event.put(column.getName(), obj);
                        }
                        collector.collect(event.toString(), event);
                    }
                }
            });
        } else {
            throw new IllegalStateException("Undefined output stream " + cepDefinition.outputStreamId);
        }
        runtime.start();
    }

    @Override
    public void transform(Map event) {
        String streamId = cepDefinition.getInputStreamId();
        InputHandler inputHandler = runtime.getInputHandler(streamId);

        if (inputHandler != null) {
            try {
                inputHandler.send(event.values().toArray());
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("sent event to siddhi stream {} ", streamId);
            }
        } else {
            LOG.warn("No input handler found for stream {}", streamId);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing handler for query {}", this.cepDefinition.getSiddhiQuery());
        this.runtime.shutdown();
        LOG.info("Shutdown siddhi runtime {}", this.runtime.getName());
        this.siddhiManager.shutdown();
        LOG.info("Shutdown siddhi manager {}", this.siddhiManager);
    }

    public static class CEPDefinition {
        private String inputStreamId;
        private String outputStreamId;
        private String siddhiQuery;

        public CEPDefinition(String siddhiQuery, String inputStreamId, String outputStreamId) {
            this.siddhiQuery = siddhiQuery;
            this.inputStreamId = inputStreamId;
            this.outputStreamId = outputStreamId;
        }

        public String getSiddhiQuery() {
            return siddhiQuery;
        }

        public void setSiddhiQuery(String siddhiQuery) {
            this.siddhiQuery = siddhiQuery;
        }

        public String getOutputStreamId() {
            return outputStreamId;
        }

        public void setOutputStreamId(String outputStreamId) {
            this.outputStreamId = outputStreamId;
        }

        public String getInputStreamId() {
            return inputStreamId;
        }

        public void setInputStreamId(String inputStreamId) {
            this.inputStreamId = inputStreamId;
        }
    }
}