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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

@RequiredArgsConstructor
@Slf4j
public class AlertStreamCallback extends StreamCallback {
    private final String outputStream;
    private final StreamDefinition definition;
    private final PolicyHandlerContext context;
    private final int currentIndex;

    /**
     * Possibly more than one event will be triggered for alerting.
     */
    @Override
    public void receive(Event[] events) {
        String policyName = context.getPolicyDefinition().getName();
        String siteId = context.getPolicyDefinition().getSiteId();
        log.info("Generated {} alerts from policy '{}' in {}, index of definiton {} ",
                events.length, policyName, context.getPolicyEvaluatorId(), currentIndex);
        for (Event e : events) {
            org.apache.eagle.flink.Collector<AlertStreamEvent> eagleCollector = (org.apache.eagle.flink.Collector<AlertStreamEvent>)e.getData()[0];
            AlertStreamEvent event = new AlertStreamEvent();
            event.setSiteId(siteId);
            event.setTimestamp(e.getTimestamp());
            event.setData(e.getData());
            event.setStreamId(outputStream);
            event.setPolicyId(context.getPolicyDefinition().getName());
            if (this.context.getPolicyEvaluator() != null) {
                event.setCreatedBy(context.getPolicyEvaluator().getName());
            }
            event.setCreatedTime(System.currentTimeMillis());
            event.setSchema(definition);

            log.debug("Generate new alert event: {}", event);
            eagleCollector.emit(event);
        }
        context.getPolicyCounter().incrBy(String.format("%s.%s", this.context.getPolicyDefinition().getName(), "alert_count"), events.length);
    }
}
