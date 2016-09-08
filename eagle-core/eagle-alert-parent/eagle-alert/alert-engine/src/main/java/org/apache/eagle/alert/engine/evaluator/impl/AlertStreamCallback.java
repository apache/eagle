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
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.CompositePolicyHandler;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created on 8/2/16.
 */
public class AlertStreamCallback extends StreamCallback {

    private static final Logger LOG = LoggerFactory.getLogger(AlertStreamCallback.class);
    private final String outputStream;
    private final Collector<AlertStreamEvent> collector;
    private final PolicyHandlerContext context;
    private final StreamDefinition definition;

    private int currentIndex;

    public AlertStreamCallback(String outputStream,
                               StreamDefinition streamDefinition,
                               Collector<AlertStreamEvent> collector,
                               PolicyHandlerContext context,
                               int currentIndex) {
        this.outputStream = outputStream;
        this.collector = collector;
        this.context = context;
        this.definition = streamDefinition;
        this.currentIndex = currentIndex;
    }

    /**
     * Possibly more than one event will be triggered for alerting
     *
     * @param events
     */
    @Override
    public void receive(Event[] events) {
        String policyName = context.getPolicyDefinition().getName();
        CompositePolicyHandler handler = ((PolicyGroupEvaluatorImpl) context.getPolicyEvaluator()).getPolicyHandler(policyName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated {} alerts from policy '{}' in {}, index of definiton {} ", events.length, policyName, context.getPolicyEvaluatorId(), currentIndex);
        }
        for (Event e : events) {
            AlertStreamEvent event = new AlertStreamEvent();
            event.setTimestamp(e.getTimestamp());
            event.setData(e.getData());
            event.setStreamId(outputStream);
            event.setPolicyId(context.getPolicyDefinition().getName());
            if (this.context.getPolicyEvaluator() != null) {
                event.setCreatedBy(context.getPolicyEvaluator().getName());
            }
            event.setCreatedTime(System.currentTimeMillis());
            event.setSchema(definition);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Generate new alert event: {}", event);
            }
            try {
                if (handler == null) {
                    // extreme case: the handler is removed from the evaluator. Just emit.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" handler not found when callback received event, directly emit. policy removed? ");
                    }
                    collector.emit(event);
                } else {
                    handler.send(event, currentIndex + 1);
                }
            } catch (Exception ex) {
                LOG.error(String.format("send event %s to index %d failed with exception. ", event, currentIndex), ex);
            }
        }
        context.getPolicyCounter().scope(String.format("%s.%s", this.context.getPolicyDefinition().getName(), "alert_count")).incrBy(events.length);
    }
}
