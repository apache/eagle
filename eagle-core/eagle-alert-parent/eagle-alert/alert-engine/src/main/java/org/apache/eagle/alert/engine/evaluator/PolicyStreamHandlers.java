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

import java.util.Map;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.absence.AbsencePolicyHandler;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiPolicyHandler;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiPolicyStateHandler;
import org.apache.eagle.alert.engine.evaluator.nodata.NoDataPolicyTimeBatchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO/FIXME: to support multiple stage definition in single policy. The methods in this class is not good to understand now.(Hard code of 0/1).
 */
public class PolicyStreamHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyStreamHandlers.class);

    public static final String SIDDHI_ENGINE = "siddhi";
    public static final String NO_DATA_ALERT_ENGINE = "nodataalert";
    public static final String ABSENCE_ALERT_ENGINE = "absencealert";
    public static final String CUSTOMIZED_ENGINE = "Custom";

    public static PolicyStreamHandler createHandler(PolicyDefinition.Definition definition, Map<String, StreamDefinition> sds) {
        if (SIDDHI_ENGINE.equals(definition.getType())) {
            return new SiddhiPolicyHandler(sds, 0);// // FIXME: 8/2/16 
        } else if (NO_DATA_ALERT_ENGINE.equals(definition.getType())) {
            // no data for an entire stream won't trigger gap alert  (use local time & batch window instead)
            return new NoDataPolicyTimeBatchHandler(sds);
        } else if (ABSENCE_ALERT_ENGINE.equals(definition.getType())) {
            return new AbsencePolicyHandler(sds);
        } else if (CUSTOMIZED_ENGINE.equals(definition.getType())) {
            try {
                Class<?> handlerClz = Class.forName(definition.getHandlerClass());
                PolicyStreamHandler handler = (PolicyStreamHandler) handlerClz.getConstructor(Map.class).newInstance(sds);
                return handler;
            } catch (Exception e) {
                LOG.error("Not able to create policy handler for handler class " + definition.getHandlerClass(), e);
                throw new IllegalArgumentException("Illegal extended policy handler class!" + definition.getHandlerClass());
            }
        }
        throw new IllegalArgumentException("Illegal policy stream handler type " + definition.getType());
    }

    public static PolicyStreamHandler createStateHandler(String type, Map<String, StreamDefinition> sds) {
        if (SIDDHI_ENGINE.equals(type)) {
            return new SiddhiPolicyStateHandler(sds, 1); //// FIXME: 8/2/16
        }
        throw new IllegalArgumentException("Illegal policy state handler type " + type);
    }
}