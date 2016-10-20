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
package org.apache.eagle.alert.engine.interpreter;

import com.google.common.base.Preconditions;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamNotDefinedException;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * PolicyInterpreter Helper Methods:
 * <ul>
 * <li>Parse: parse siddhi query and generate static execution plan</li>
 * <li>Validate: validate policy definition with execution plan and metadata</li>
 * </ul>
 *
 * @see PolicyExecutionPlanner
 * @see <a href="https://docs.wso2.com/display/CEP300/WSO2+Complex+Event+Processor+Documentation">WSO2 Complex Event Processor Documentation</a>
 */
public class PolicyInterpreter {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyInterpreter.class);

    /**
     * See https://docs.wso2.com/display/CEP300/Windows#Windows-ExternalTimeWindow.
     */
    private static final String WINDOW_EXTERNAL_TIME = "externalTime";

    public static PolicyParseResult parse(String executionPlanQuery) {
        PolicyParseResult policyParseResult = new PolicyParseResult();
        try {
            policyParseResult.setPolicyExecutionPlan(parseExecutionPlan(executionPlanQuery));
            policyParseResult.setSuccess(true);
            policyParseResult.setMessage("Parsed successfully");
        } catch (Exception exception) {
            LOG.error("Got error to parse policy: {}", executionPlanQuery, exception);
            policyParseResult.setSuccess(false);
            policyParseResult.setMessage(exception.getMessage());
            policyParseResult.setStackTrace(exception);
        }
        return policyParseResult;
    }

    /**
     * Quick parseExecutionPlan policy.
     */
    public static PolicyExecutionPlan parseExecutionPlan(String policyDefinition, Map<String, StreamDefinition> inputStreamDefinitions) throws Exception {
        // Validate inputStreams are valid
        Preconditions.checkNotNull(inputStreamDefinitions, "No inputStreams to connect from");
        return parseExecutionPlan(SiddhiDefinitionAdapter.buildSiddhiExecutionPlan(policyDefinition, inputStreamDefinitions));
    }

    public static PolicyExecutionPlan parseExecutionPlan(String executionPlanQuery) throws Exception {
        return new PolicyExecutionPlannerImpl(executionPlanQuery).getExecutionPlan();
    }

    public static PolicyValidationResult validate(PolicyDefinition policy, Map<String, StreamDefinition> allDefinitions) {
        Map<String, StreamDefinition> inputDefinitions = new HashMap<>();
        PolicyValidationResult policyValidationResult = new PolicyValidationResult();
        policyValidationResult.setPolicyDefinition(policy);
        try {
            if (policy.getInputStreams() != null) {
                for (String streamId : policy.getInputStreams()) {
                    if (allDefinitions.containsKey(streamId)) {
                        inputDefinitions.put(streamId, allDefinitions.get(streamId));
                    } else {
                        throw new StreamNotDefinedException(streamId);
                    }
                }
            }

            PolicyExecutionPlan policyExecutionPlan = parseExecutionPlan(policy.getDefinition().getValue(), inputDefinitions);
            // Validate output
            if (policy.getOutputStreams() != null) {
                for (String outputStream : policy.getOutputStreams()) {
                    if (!policyExecutionPlan.getOutputStreams().containsKey(outputStream)) {
                        throw new StreamNotDefinedException("Output stream " + outputStream + " not defined");
                    }
                }
            }
            policyValidationResult.setPolicyExecutionPlan(policyExecutionPlan);
            policyValidationResult.setSuccess(true);
            policyValidationResult.setMessage("Validated successfully");
        } catch (Exception exception) {
            LOG.error("Got error to validate policy definition: {}", policy, exception);
            policyValidationResult.setSuccess(false);
            policyValidationResult.setMessage(exception.getMessage());
            policyValidationResult.setStackTrace(exception);
        }

        return policyValidationResult;
    }
}