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
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.execution.query.output.stream.OutputStream;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.TimeConstant;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PolicyCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyCompiler.class);

    /**
     * Quick parseExecutionPlan policy.
     */
    public static PolicyExecutionPlan parseExecutionPlan(String policyDefinition, Map<String, StreamDefinition> inputStreamDefinitions) throws Exception {
        // Validate inputStreams are valid
        Preconditions.checkNotNull(inputStreamDefinitions, "No inputStreams to connect from");
        return parseExecutionPlan(SiddhiDefinitionAdapter.buildSiddhiExecutionPlan(policyDefinition, inputStreamDefinitions));
    }

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

    public static PolicyExecutionPlan parseExecutionPlan(String executionPlanQuery) throws Exception {
        PolicyExecutionPlan policyExecutionPlan = new PolicyExecutionPlan();
        try {
            ExecutionPlan executionPlan = SiddhiCompiler.parse(executionPlanQuery);

            policyExecutionPlan.setExecutionPlanDesc(executionPlan.toString());

            // Set current execution plan as valid
            policyExecutionPlan.setExecutionPlanSource(executionPlanQuery);
            policyExecutionPlan.setInternalExecutionPlan(executionPlan);

            Map<String, List<StreamColumn>> actualInputStreams = new HashMap<>();
            Map<String, List<StreamColumn>> actualOutputStreams = new HashMap<>();
            List<StreamPartition> partitions = new ArrayList<>();

            // Go through execution element
            for (ExecutionElement executionElement : executionPlan.getExecutionElementList()) {
                if (executionElement instanceof Query) {
                    // -------------
                    // Explain Query
                    // -------------

                    // Input streams
                    InputStream inputStream = ((Query) executionElement).getInputStream();
                    Selector selector = ((Query) executionElement).getSelector();

                    for (String streamId: inputStream.getUniqueStreamIds()) {
                        if (!actualInputStreams.containsKey(streamId)) {
                            org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition = executionPlan.getStreamDefinitionMap().get(streamId);
                            if (streamDefinition != null) {
                                actualInputStreams.put(streamId, SiddhiDefinitionAdapter.convertFromSiddiDefinition(streamDefinition).getColumns());
                            } else {
                                actualInputStreams.put(streamId,null);
                            }
                        }
                    }

                    // Window Spec and Partition
                    if (inputStream instanceof SingleInputStream) {
                        // Window Spec
                        List<Window> windows = new ArrayList<>();
                        for (StreamHandler streamHandler : ((SingleInputStream) inputStream).getStreamHandlers()) {
                            if (streamHandler instanceof Window) {
                                windows.add((Window) streamHandler);
                            }
                        }

                        // Group By Spec
                        List<Variable> groupBy = selector.getGroupByList();

                        if (windows.size() > 0 || groupBy.size() >= 0) {
                            partitions.add(convertSingleStreamWindowAndGroupByToPartition(((SingleInputStream) inputStream).getStreamId(),windows,groupBy));
                        }
                    } else if(inputStream instanceof JoinInputStream) {
                        // TODO: Parse multiple stream join

                    } else if(inputStream instanceof StateInputStream) {
                        // TODO: Parse StateInputStream
                    }

                    // Output streams
                    OutputStream outputStream = ((Query) executionElement).getOutputStream();
                    actualOutputStreams.put(outputStream.getId(), convertOutputStreamColumns(selector.getSelectionList()));
                } else {
                    LOG.warn("Unhandled execution element: {}", executionElement.toString());
                }
            }
            // Set used input streams
            policyExecutionPlan.setInputStreams(actualInputStreams);

            // Set Partitions
            policyExecutionPlan.setStreamPartitions(partitions);

            // Validate outputStreams
            policyExecutionPlan.setOutputStreams(actualOutputStreams);
        } catch (Exception ex) {
            LOG.error("Got error to parseExecutionPlan policy execution plan: \n{}", executionPlanQuery, ex);
            throw ex;
        }
        return policyExecutionPlan;
    }

    private static StreamPartition convertSingleStreamWindowAndGroupByToPartition(String streamId, List<Window> windows, List<Variable> groupBy) {
        StreamPartition partition = new StreamPartition();
        partition.setStreamId(streamId);
        StreamSortSpec sortSpec = null;

        if (windows.size() > 0) {
            sortSpec = new StreamSortSpec();
            for (Window window:windows) {
                if (window.getFunction().equals("timeBatch")) {
                    sortSpec.setWindowPeriodMillis(((TimeConstant) window.getParameters()[0]).getValue().intValue());
                    sortSpec.setWindowMargin(sortSpec.getWindowPeriodMillis() / 3);
                }
            }
        }
        partition.setSortSpec(sortSpec);
        if (groupBy.size() > 0) {
            partition.setColumns(groupBy.stream().map(Variable::getAttributeName).collect(Collectors.toList()));
            partition.setType(StreamPartition.Type.GROUPBY);
        } else {
            partition.setType(StreamPartition.Type.SHUFFLE);
        }
        return partition;
    }

    public static PolicyValidationResult validate(PolicyDefinition policy, IMetadataDao metadataDao) {
        Map<String, StreamDefinition> allDefinitions = new HashMap<>();
        for (StreamDefinition definition : metadataDao.listStreams()) {
            allDefinitions.put(definition.getStreamId(), definition);
        }
        return validate(policy, allDefinitions);
    }

    public static PolicyValidationResult validate(PolicyDefinition policy, Map<String, StreamDefinition> allDefinitions) {
        Map<String, StreamDefinition> inputDefinitions = new HashMap<>();
        PolicyValidationResult policyValidationResult = new PolicyValidationResult();
        policyValidationResult.setPolicyDefinition(policy);
        try {
            if (policy.getInputStreams() != null ) {
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

    private static List<StreamColumn> convertOutputStreamColumns(List<OutputAttribute> outputAttributeList) {
        return outputAttributeList.stream().map(outputAttribute -> {
            StreamColumn streamColumn = new StreamColumn();
            streamColumn.setName(outputAttribute.getRename());
            streamColumn.setDescription(outputAttribute.getExpression().toString());
            return streamColumn;
        }).collect(Collectors.toList());
    }
}