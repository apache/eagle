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

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinitionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created on 7/27/16.
 */
public class SiddhiPolicyStateHandler extends SiddhiPolicyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPolicyStateHandler.class);

    public SiddhiPolicyStateHandler(Map<String, StreamDefinition> sds, int index) {
        super(sds, index);
    }

    @Override
    protected String generateExecutionPlan(PolicyDefinition policyDefinition, Map<String, StreamDefinition> sds) throws StreamDefinitionNotFoundException {
        StringBuilder builder = new StringBuilder();
        PolicyDefinition.Definition stateDefiniton = policyDefinition.getStateDefinition();
        for (String inputStream : stateDefiniton.getInputStreams()) { // the state stream follow the output stream of the policy definition
            builder.append(SiddhiDefinitionAdapter.buildStreamDefinition(sds.get(inputStream)));
            builder.append("\n");
        }
        builder.append(stateDefiniton.value);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated siddhi state execution plan: {} from definiton: {}", builder.toString(), stateDefiniton);
        }
        return builder.toString();
    }

    @Override
    protected List<String> getOutputStreams(PolicyDefinition policy) {
        return policy.getStateDefinition().getOutputStreams();
    }

    // more validation on prepare

}
