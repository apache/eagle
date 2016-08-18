/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.utils;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;

import java.util.Map;

/**
 * Created on 8/16/16.
 */
public class AlertStreamUtils {

    /**
     * Create alert stream event for publisher.
     */
    public static AlertStreamEvent createAlertEvent(StreamEvent event,
                                                    PolicyHandlerContext context,
                                                    Map<String, StreamDefinition> sds) {
        PolicyDefinition policyDef = context.getPolicyDefinition();
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();

        alertStreamEvent.setTimestamp(event.getTimestamp());
        alertStreamEvent.setData(event.getData());
        alertStreamEvent.setStreamId(policyDef.getOutputStreams().get(0));
        alertStreamEvent.setPolicy(policyDef);

        if (context.getPolicyEvaluator() != null) {
            alertStreamEvent.setCreatedBy(context.getPolicyEvaluator().getName());
        }

        alertStreamEvent.setCreatedTime(System.currentTimeMillis());

        String is = policyDef.getInputStreams().get(0);
        StreamDefinition sd = sds.get(is);
        alertStreamEvent.setSchema(sd);

        return alertStreamEvent;
    }
}
