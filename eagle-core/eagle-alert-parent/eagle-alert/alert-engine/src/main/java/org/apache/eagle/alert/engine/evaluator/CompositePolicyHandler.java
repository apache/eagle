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
package org.apache.eagle.alert.engine.evaluator;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 7/27/16.
 */
public class CompositePolicyHandler implements PolicyStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CompositePolicyHandler.class);

    private PolicyStreamHandler policyHandler;
    private PolicyStreamHandler stateHandler;
    private List<PolicyStreamHandler> handlers = new ArrayList<>();

    private Collector<AlertStreamEvent> collector;

    private Map<String, StreamDefinition> sds;

    public CompositePolicyHandler(Map<String, StreamDefinition> sds) {
        this.sds = sds;
    }

    @Override
    public void prepare(Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
        this.collector = collector;
        // TODO: create two handlers
        policyHandler = PolicyStreamHandlers.createHandler(context.getPolicyDefinition().getDefinition(), sds);
        policyHandler.prepare(collector, context);
        handlers.add(policyHandler);

        if (context.getPolicyDefinition().getStateDefinition() != null) {
            stateHandler = PolicyStreamHandlers.createStateHandler(context.getPolicyDefinition().getStateDefinition().type, sds);
            stateHandler.prepare(collector, context);
            handlers.add(stateHandler);
        }
    }

    @Override
    public void send(StreamEvent event) throws Exception {
//        policyHandler.send(event);
        send(event, 0);
    }

    // send event to index of stream handler
    public void send(StreamEvent event, int idx) throws Exception {
        if (handlers.size() > idx) {
            handlers.get(idx).send(event);
        } else if (event instanceof AlertStreamEvent) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Emit new alert event: {}", event);
            }
            collector.emit((AlertStreamEvent) event); // for alert stream events, emit if no handler found.
        } else {
            // nothing found. LOG, and throw exception
            LOG.error("non-alert-stream-event {} send with index {}, but the handler is not found!", event, idx);
            throw new Exception(String.format("event %s send with idx %d can not found expecting handler!", event, idx));
        }
    }

    @Override
    public void close() throws Exception {
        for (PolicyStreamHandler handler : handlers) {
            try {
                handler.close();
            } catch (Exception e) {
                LOG.error("close handler {} failed, continue to run.", handler);
            }
        }
    }

}
