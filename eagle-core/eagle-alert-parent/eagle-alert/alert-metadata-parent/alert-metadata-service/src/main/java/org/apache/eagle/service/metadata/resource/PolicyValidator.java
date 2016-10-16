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
package org.apache.eagle.service.metadata.resource;

import com.google.common.base.Preconditions;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamNotDefinedException;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter;
import org.apache.eagle.alert.metadata.IMetadataDao;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.HashMap;
import java.util.Map;

public class PolicyValidator {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyValidator.class);

    public static PolicyValidation validate(PolicyDefinition policy, Map<String,StreamDefinition> allStreamDefinitions) {
        PolicyValidation policyValidation = new PolicyValidation();
        policyValidation.setPolicyDefinition(policy);

        SiddhiManager siddhiManager = null;
        ExecutionPlanRuntime executionRuntime = null;
        String executionPlan = null;

        try {
            // Validate inputStreams are valid
            Preconditions.checkNotNull(policy.getInputStreams(), "No inputStreams to connect from");
            Map<String,StreamDefinition> currentDefinitions = new HashMap<>();
            for (String streamId : policy.getInputStreams()) {
                if (allStreamDefinitions.containsKey(streamId)) {
                    currentDefinitions.put(streamId, allStreamDefinitions.get(streamId));
                } else {
                    throw new StreamNotDefinedException(streamId);
                }
            }

            // Build final execution plan
            executionPlan = SiddhiDefinitionAdapter.buildSiddhiExecutionPlan(policy, currentDefinitions);
            siddhiManager = new SiddhiManager();
            executionRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

            // Set current execution plan as valid
            policyValidation.setValidExecutionPlan(executionPlan);

            // Siddhi runtime active stream definitions
            Map<String,AbstractDefinition> definitionMap = executionRuntime.getStreamDefinitionMap();

            Map<String,StreamDefinition> validInputStreams = new HashMap<>();
            Map<String,StreamDefinition> validOutputStreams = new HashMap<>();

            for (Map.Entry<String,AbstractDefinition> entry : definitionMap.entrySet()) {
                if (currentDefinitions.containsKey(entry.getKey())) {
                    validInputStreams.put(entry.getKey(),currentDefinitions.get(entry.getKey()));
                } else {
                    validOutputStreams.put(entry.getKey(),SiddhiDefinitionAdapter.convertFromSiddiDefinition(entry.getValue()));
                }
            }
            policyValidation.setValidInputStreams(validInputStreams);

            // Validate outputStreams
            policyValidation.setValidOutputStreams(validOutputStreams);
            if (policy.getOutputStreams()!=null) {
                for (String outputStream : policy.getOutputStreams()) {
                    if (!validOutputStreams.containsKey(outputStream)) {
                        throw new StreamNotDefinedException("Output stream " + outputStream + " not defined");
                    }
                }
            }

            // TODO: Validate partitions

            policyValidation.setSuccess(true);
            policyValidation.setMessage("Validation success");
        } catch (SiddhiParserException parserException) {
            LOG.error("Got error to parse policy execution plan: \n{}", executionPlan, parserException);
            policyValidation.setSuccess(false);
            policyValidation.setMessage("Parser Error: "+parserException.getMessage());
            policyValidation.setException(ExceptionUtils.getStackTrace(parserException));
        } catch (Exception exception) {
            LOG.error("Got Error to validate policy definition", exception);
            policyValidation.setSuccess(false);
            policyValidation.setMessage("Validation Error: "+exception.getMessage());
            policyValidation.setException(ExceptionUtils.getStackTrace(exception));
        } finally {
            if (executionRuntime!=null) {
                executionRuntime.shutdown();
            }
            if (siddhiManager!=null) {
                siddhiManager.shutdown();
            }
        }
        return policyValidation;
    }

    public static PolicyValidation validate(PolicyDefinition policy, IMetadataDao metadataDao) {
        Map<String,StreamDefinition> allDefinitions = new HashMap<>();
        for (StreamDefinition definition:metadataDao.listStreams()){
            allDefinitions.put(definition.getStreamId(),definition);
        }
        return validate(policy,allDefinitions);
    }
}