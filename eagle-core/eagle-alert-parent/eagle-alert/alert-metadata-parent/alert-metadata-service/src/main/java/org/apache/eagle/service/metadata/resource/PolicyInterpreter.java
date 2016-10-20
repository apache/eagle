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
import org.apache.commons.collections.ListUtils;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.exception.DefinitionNotExistException;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.exception.DuplicateDefinitionException;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
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
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.condition.Compare;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.expression.constant.TimeConstant;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.util.*;
import java.util.stream.Collectors;

/**
 * PolicyInterpreter:
 * <ul>
 * <li>Parse: parse siddhi query and generate static execution plan</li>
 * <li>Validate: validate policy definition with execution plan and metadata</li>
 * </ul>
 *
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

    /**
     * The Core Function to parse Siddhi Execution Plan Query to Distributed Execution Plan.
     */
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
            Map<String, StreamPartition> partitions = new HashMap<>();

            // Go through execution element
            for (ExecutionElement executionElement : executionPlan.getExecutionElementList()) {
                // -------------
                // Explain Query
                // -------------
                if (executionElement instanceof Query) {
                    // -----------------------
                    // Query Level Variables
                    // -----------------------
                    InputStream inputStream = ((Query) executionElement).getInputStream();
                    Selector selector = ((Query) executionElement).getSelector();
                    Map<String, SingleInputStream> queryAliasToStreamMapping = new HashMap<>();

                    // Inputs stream definitions
                    for (String streamId : inputStream.getUniqueStreamIds()) {
                        if (!actualInputStreams.containsKey(streamId)) {
                            org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition = executionPlan.getStreamDefinitionMap().get(streamId);
                            if (streamDefinition != null) {
                                actualInputStreams.put(streamId, SiddhiDefinitionAdapter.convertFromSiddiDefinition(streamDefinition).getColumns());
                            } else {
                                actualInputStreams.put(streamId, null);
                            }
                        }
                    }

                    // Window Spec and Partition
                    if (inputStream instanceof SingleInputStream) {
                        retrieveAlias((SingleInputStream) inputStream, queryAliasToStreamMapping);
                        retrievePartition(findStreamPartition((SingleInputStream) inputStream, selector), partitions);
                    } else {
                        if (inputStream instanceof JoinInputStream) {
                            // Only Support JOIN/INNER_JOIN Now
                            if (((JoinInputStream) inputStream).getType().equals(JoinInputStream.Type.INNER_JOIN) || ((JoinInputStream) inputStream).getType().equals(JoinInputStream.Type.JOIN)) {
                                SingleInputStream leftInputStream = (SingleInputStream) ((JoinInputStream) inputStream).getLeftInputStream();
                                SingleInputStream rightInputStream = (SingleInputStream) ((JoinInputStream) inputStream).getRightInputStream();

                                retrievePartition(findStreamPartition(leftInputStream, selector), partitions);
                                retrievePartition(findStreamPartition(rightInputStream, selector), partitions);
                                retrieveAlias(leftInputStream, queryAliasToStreamMapping);
                                retrieveAlias(rightInputStream, queryAliasToStreamMapping);

                            } else {
                                throw new ExecutionPlanValidationException("Not support " + ((JoinInputStream) inputStream).getType() + " yet, currently support: INNER JOIN");
                            }

                            Expression joinCondition = ((JoinInputStream) inputStream).getOnCompare();

                            if (joinCondition != null) {
                                if (joinCondition instanceof Compare) {
                                    if (((Compare) joinCondition).getOperator().equals(Compare.Operator.EQUAL)) {
                                        Variable leftExpression = (Variable) ((Compare) joinCondition).getLeftExpression();
                                        Preconditions.checkNotNull(leftExpression.getStreamId());
                                        Preconditions.checkNotNull(leftExpression.getAttributeName());

                                        StreamPartition leftPartition = new StreamPartition();
                                        leftPartition.setType(StreamPartition.Type.GROUPBY);
                                        leftPartition.setColumns(Collections.singletonList(leftExpression.getAttributeName()));
                                        leftPartition.setStreamId(retrieveStreamId(leftExpression,actualInputStreams,queryAliasToStreamMapping));
                                        retrievePartition(leftPartition, partitions);

                                        Variable rightExpression = (Variable) ((Compare) joinCondition).getRightExpression();
                                        Preconditions.checkNotNull(rightExpression.getStreamId());
                                        Preconditions.checkNotNull(rightExpression.getAttributeName());
                                        StreamPartition rightPartition = new StreamPartition();
                                        rightPartition.setType(StreamPartition.Type.GROUPBY);
                                        rightPartition.setColumns(Collections.singletonList(rightExpression.getAttributeName()));
                                        rightPartition.setStreamId(retrieveStreamId(rightExpression,actualInputStreams,queryAliasToStreamMapping));
                                        retrievePartition(leftPartition, partitions);
                                    } else {
                                        throw new ExecutionPlanValidationException("Only support \"EQUAL\" condition in INNER JOIN" + joinCondition);
                                    }
                                } else {
                                    throw new ExecutionPlanValidationException("Only support \"Compare\" on INNER JOIN condition in INNER JOIN: " + joinCondition);
                                }
                            }
                        } else if (inputStream instanceof StateInputStream) {
                            // Group By Spec
                            List<Variable> groupBy = selector.getGroupByList();
                            if (groupBy.size() >= 0) {
                                Map<String, List<Variable>> streamGroupBy = new HashMap<>();
                                for (String streamId : inputStream.getUniqueStreamIds()) {
                                    streamGroupBy.put(streamId, new ArrayList<>());
                                }
                                for (Variable variable : groupBy) {
                                    // Not stream not set, then should be all streams' same field
                                    if (variable.getStreamId() == null) {
                                        for (String streamId : inputStream.getUniqueStreamIds()) {
                                            streamGroupBy.get(streamId).add(variable);
                                        }
                                    } else {
                                        if (streamGroupBy.containsKey(variable.getStreamId())) {
                                            streamGroupBy.get(variable.getStreamId()).add(variable);
                                        } else {
                                            throw new AliasNotSupportException("Not support groupBy alias \"" + variable.getStreamId() + "." + variable.getAttributeName() + "\" yet");
                                        }
                                    }
                                }
                                for (Map.Entry<String, List<Variable>> entry : streamGroupBy.entrySet()) {
                                    if (entry.getValue().size() > 0) {
                                        retrievePartition(generatePartition(entry.getKey(), null, Arrays.asList(entry.getValue().toArray(new Variable[entry.getValue().size()]))), partitions);
                                    }
                                }
                            }
                        }
                    }

                    // Output streams
                    OutputStream outputStream = ((Query) executionElement).getOutputStream();
                    actualOutputStreams.put(outputStream.getId(), convertOutputStreamColumns(selector.getSelectionList()));
                } else {
                    LOG.warn("Unhandled execution element: {}", executionElement.toString());
                }
            }
            // Set effective input streams
            policyExecutionPlan.setInputStreams(actualInputStreams);

            // Set effective output streams
            policyExecutionPlan.setOutputStreams(actualOutputStreams);

            // Set Partitions
            for (String streamId : actualInputStreams.keySet()) {
                // Use shuffle partition by default
                if (!partitions.containsKey(streamId)) {
                    StreamPartition shufflePartition = new StreamPartition();
                    shufflePartition.setStreamId(streamId);
                    shufflePartition.setType(StreamPartition.Type.SHUFFLE);
                    partitions.put(streamId, shufflePartition);
                }
            }
            policyExecutionPlan.setStreamPartitions(new ArrayList<>(partitions.values()));
        } catch (Exception ex) {
            LOG.error("Got error to parse policy execution plan: \n{}", executionPlanQuery, ex);
            throw ex;
        }
        return policyExecutionPlan;
    }


    private static String retrieveStreamId(Variable variable, Map<String, List<StreamColumn>> streamMap, Map<String, SingleInputStream> aliasMap) {
        Preconditions.checkNotNull(variable.getStreamId(), "streamId");
        if (streamMap.containsKey(variable.getStreamId()) && aliasMap.containsKey(variable.getStreamId())) {
            throw new DuplicateDefinitionException("Duplicated streamId and alias: " + variable.getStreamId());
        } else if (streamMap.containsKey(variable.getStreamId())) {
            return variable.getStreamId();
        } else if (aliasMap.containsKey(variable.getStreamId())) {
            return aliasMap.get(variable.getStreamId()).getStreamId();
        } else {
            throw new DefinitionNotExistException(variable.getStreamId());
        }
    }

    private static StreamPartition findStreamPartition(SingleInputStream inputStream, Selector selector) {
        // Window Spec
        List<Window> windows = new ArrayList<>();
        for (StreamHandler streamHandler : inputStream.getStreamHandlers()) {
            if (streamHandler instanceof Window) {
                windows.add((Window) streamHandler);
            }
        }

        // Group By Spec
        List<Variable> groupBy = selector.getGroupByList();
        if (windows.size() > 0 || groupBy.size() >= 0) {
            return generatePartition(inputStream.getStreamId(), windows, groupBy);
        } else {
            return null;
        }
    }

    private static void retrievePartition(StreamPartition partition, Map<String, StreamPartition> repo) {
        if (partition == null) {
            return;
        }

        if (!repo.containsKey(partition.getStreamId())) {
            repo.put(partition.getStreamId(), partition);
        } else if (!repo.get(partition.getStreamId()).equals(partition)) {
            StreamPartition existingPartition = repo.get(partition.getStreamId());
            // If same Type & Columns but different sort spec, then use larger
            if (existingPartition.getType().equals(partition.getType())
                && ListUtils.isEqualList(existingPartition.getColumns(), partition.getColumns())
                && partition.getSortSpec().getWindowPeriodMillis() > existingPartition.getSortSpec().getWindowPeriodMillis()
                || existingPartition.getType().equals(StreamPartition.Type.SHUFFLE)) {
                repo.put(partition.getStreamId(), partition);
            } else {
                // Throw exception as it unable to conflict partitions on same stream will not be able to run in distributed mode
                throw new ExecutionPlanValidationException("You have incompatible partitions on stream " + partition.getStreamId()
                    + ": [1] " + repo.get(partition.getStreamId()).toString() + " [2] " + partition.toString() + "");
            }
        }
    }

    private static void retrieveAlias(SingleInputStream inputStream, Map<String, SingleInputStream> aliasStreamMapping) {
        if (inputStream.getStreamReferenceId() != null) {
            if (aliasStreamMapping.containsKey(inputStream.getStreamReferenceId())) {
                throw new ExecutionPlanValidationException("Duplicated stream alias " + inputStream.getStreamId() + " -> " + inputStream);
            } else {
                aliasStreamMapping.put(inputStream.getStreamReferenceId(), inputStream);
            }
        }
    }

    private static StreamPartition generatePartition(String streamId, List<Window> windows, List<Variable> groupBy) {
        StreamPartition partition = new StreamPartition();
        partition.setStreamId(streamId);
        StreamSortSpec sortSpec = null;
        if (windows != null && windows.size() > 0) {
            for (Window window : windows) {
                if (window.getFunction().equals(WINDOW_EXTERNAL_TIME)) {
                    sortSpec = new StreamSortSpec();
                    sortSpec.setWindowPeriodMillis(getExternalTimeWindowSize(window));
                    sortSpec.setWindowMargin(sortSpec.getWindowPeriodMillis() / 5);
                }
            }
        }
        partition.setSortSpec(sortSpec);
        if (groupBy != null && groupBy.size() > 0) {
            partition.setColumns(groupBy.stream().map(Variable::getAttributeName).collect(Collectors.toList()));
            partition.setType(StreamPartition.Type.GROUPBY);
        } else {
            partition.setType(StreamPartition.Type.SHUFFLE);
        }
        return partition;
    }

    private static int getExternalTimeWindowSize(Window window) {
        Expression windowSize = window.getParameters()[1];
        if (windowSize instanceof TimeConstant) {
            return ((TimeConstant) windowSize).getValue().intValue();
        } else if (windowSize instanceof IntConstant) {
            return ((IntConstant) windowSize).getValue();
        } else if (windowSize instanceof LongConstant) {
            return ((LongConstant) windowSize).getValue().intValue();
        } else {
            throw new UnsupportedOperationException("Illegal type of window size expression:" + windowSize.toString());
        }
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

    private static List<StreamColumn> convertOutputStreamColumns(List<OutputAttribute> outputAttributeList) {
        return outputAttributeList.stream().map(outputAttribute -> {
            StreamColumn streamColumn = new StreamColumn();
            streamColumn.setName(outputAttribute.getRename());
            streamColumn.setDescription(outputAttribute.getExpression().toString());
            return streamColumn;
        }).collect(Collectors.toList());
    }

    static class AliasNotSupportException extends RuntimeException {
        public AliasNotSupportException(String message) {
            super(message);
        }
    }
}