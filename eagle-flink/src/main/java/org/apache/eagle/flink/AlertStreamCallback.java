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
package org.apache.eagle.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Get called back by Siddhi runtime when policy is evaluated to be true, then
 * it will forward the alert to next processor in Flink
 */
public class AlertStreamCallback extends StreamCallback {

    private static final Logger LOG = LoggerFactory.getLogger(AlertStreamCallback.class);
    private final String outputStream;
    private final PolicyHandlerContext context;
    private final StreamDefinition definition;

    private int currentIndex;

    public AlertStreamCallback(String outputStream,
                               StreamDefinition streamDefinition,
                               PolicyHandlerContext context,
                               int currentIndex) {
        this.outputStream = outputStream;
        this.context = context;
        this.definition = streamDefinition;
        this.currentIndex = currentIndex;
    }

    /**
     * Possibly more than one event will be triggered for alerting.
     */
    @Override
    public void receive(Event[] events) {
        LOG.info("Generated {} alerts in {}, index of definiton {} ", events.length, context.getPolicyEvaluatorId(), currentIndex);
        for (Event e : events) {
            org.apache.flink.util.Collector<AlertStreamEvent> eagleCollector = (org.apache.flink.util.Collector) e.getData()[0];
            AlertStreamEvent event = new AlertStreamEvent();
            event.setSiteId("");
            event.setTimestamp(e.getTimestamp());
            // remove collector from event
            Object[] payload = new Object[e.getData().length - 1];
            System.arraycopy(e.getData(), 1, payload, 0, e.getData().length - 1);
            event.setData(payload);
            event.setStreamId(outputStream);
            event.setPolicyId("");
            if (this.context.getPolicyEvaluator() != null) {
                event.setCreatedBy(context.getPolicyEvaluator().getName());
            }
            event.setCreatedTime(System.currentTimeMillis());
            event.setSchema(definition);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Generate new alert event: {}", event);
            }
            eagleCollector.collect(event);
        }
    }
}
