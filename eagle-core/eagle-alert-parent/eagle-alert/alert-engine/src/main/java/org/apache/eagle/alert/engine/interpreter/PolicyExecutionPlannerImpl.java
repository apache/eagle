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
import org.apache.commons.collections.ListUtils;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.evaluator.impl.SiddhiDefinitionAdapter;
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
import org.wso2.siddhi.query.api.execution.query.input.state.*;
import org.wso2.siddhi.query.api.execution.query.input.stream.*;
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

class PolicyExecutionPlannerImpl implements PolicyExecutionPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyExecutionPlannerImpl.class);

    /**
     * See https://docs.wso2.com/display/CEP300/Windows#Windows-ExternalTimeWindow.
     */
    private static final String WINDOW_EXTERNAL_TIME = "externalTime";

    private final String executionPlan;
    private final Map<String,List<StreamColumn>> effectiveInputStreams;
    private final Map<String, String> effectiveInputStreamsAlias;
    private final Map<String,List<StreamColumn>> effectiveOutputStreams;
    private final Map<String,StreamPartition> effectivePartitions;
    private final PolicyExecutionPlan policyExecutionPlan;

    public PolicyExecutionPlannerImpl(String executionPlan) throws Exception {
        this.executionPlan = executionPlan;
        this.effectiveInputStreams = new HashMap<>();
        this.effectiveInputStreamsAlias = new HashMap<>();
        this.effectiveOutputStreams = new HashMap<>();
        this.effectivePartitions = new HashMap<>();
        this.policyExecutionPlan = doParse();
    }

    @Override
    public PolicyExecutionPlan getExecutionPlan() {
        return policyExecutionPlan;
    }

    private PolicyExecutionPlan doParse()  throws Exception {
        PolicyExecutionPlan policyExecutionPlan = new PolicyExecutionPlan();
        try {
            ExecutionPlan executionPlan = SiddhiCompiler.parse(this.executionPlan);

            policyExecutionPlan.setExecutionPlanDesc(executionPlan.toString());

            // Set current execution plan as valid
            policyExecutionPlan.setExecutionPlanSource(this.executionPlan);
            policyExecutionPlan.setInternalExecutionPlan(executionPlan);


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
                    Map<String, SingleInputStream> queryLevelAliasToStreamMapping = new HashMap<>();

                    // Inputs stream definitions
                    for (String streamId : inputStream.getUniqueStreamIds()) {
                        if (!effectiveInputStreams.containsKey(streamId)) {
                            org.wso2.siddhi.query.api.definition.StreamDefinition streamDefinition = executionPlan.getStreamDefinitionMap().get(streamId);
                            if (streamDefinition != null) {
                                effectiveInputStreams.put(streamId, SiddhiDefinitionAdapter.convertFromSiddiDefinition(streamDefinition).getColumns());
                            } else {
                                effectiveInputStreams.put(streamId, null);
                            }
                        }
                    }

                    // Window Spec and Partition
                    if (inputStream instanceof SingleInputStream) {
                        retrieveAliasForQuery((SingleInputStream) inputStream, queryLevelAliasToStreamMapping);
                        retrievePartition(findStreamPartition((SingleInputStream) inputStream, selector));
                    } else {
                        if (inputStream instanceof JoinInputStream) {
                            // Only Support JOIN/INNER_JOIN Now
                            if (((JoinInputStream) inputStream).getType().equals(JoinInputStream.Type.INNER_JOIN) || ((JoinInputStream) inputStream).getType().equals(JoinInputStream.Type.JOIN)) {
                                SingleInputStream leftInputStream = (SingleInputStream) ((JoinInputStream) inputStream).getLeftInputStream();
                                SingleInputStream rightInputStream = (SingleInputStream) ((JoinInputStream) inputStream).getRightInputStream();

                                retrievePartition(findStreamPartition(leftInputStream, selector));
                                retrievePartition(findStreamPartition(rightInputStream, selector));
                                retrieveAliasForQuery(leftInputStream, queryLevelAliasToStreamMapping);
                                retrieveAliasForQuery(rightInputStream, queryLevelAliasToStreamMapping);

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
                                        leftPartition.setStreamId(retrieveStreamId(leftExpression, effectiveInputStreams,queryLevelAliasToStreamMapping));
                                        retrievePartition(leftPartition);

                                        Variable rightExpression = (Variable) ((Compare) joinCondition).getRightExpression();
                                        Preconditions.checkNotNull(rightExpression.getStreamId());
                                        Preconditions.checkNotNull(rightExpression.getAttributeName());
                                        StreamPartition rightPartition = new StreamPartition();
                                        rightPartition.setType(StreamPartition.Type.GROUPBY);
                                        rightPartition.setColumns(Collections.singletonList(rightExpression.getAttributeName()));
                                        rightPartition.setStreamId(retrieveStreamId(rightExpression, effectiveInputStreams,queryLevelAliasToStreamMapping));
                                        retrievePartition(leftPartition);
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

                                collectStreamReferenceIdMapping(((StateInputStream)inputStream).getStateElement());

                                for (Variable variable : groupBy) {
                                    // Not stream not set, then should be all streams' same field
                                    if (variable.getStreamId() == null) {
                                        for (String streamId : inputStream.getUniqueStreamIds()) {
                                            streamGroupBy.get(streamId).add(variable);
                                        }
                                    } else {
                                        String streamId = variable.getStreamId();
                                        if (!this.effectiveInputStreamsAlias.containsKey(streamId)) {
                                            streamId = retrieveStreamId(variable, effectiveInputStreams,queryLevelAliasToStreamMapping);
                                        } else {
                                            streamId = this.effectiveInputStreamsAlias.get(streamId);
                                        }
                                        if (streamGroupBy.containsKey(streamId)) {
                                            streamGroupBy.get(streamId).add(variable);
                                        } else {
                                            throw new DefinitionNotExistException(streamId);
                                        }
                                    }
                                }
                                for (Map.Entry<String, List<Variable>> entry : streamGroupBy.entrySet()) {
                                    if (entry.getValue().size() > 0) {
                                        retrievePartition(generatePartition(entry.getKey(), null, Arrays.asList(entry.getValue().toArray(new Variable[entry.getValue().size()]))));
                                    }
                                }
                            }
                        }
                    }

                    // Output streams
                    OutputStream outputStream = ((Query) executionElement).getOutputStream();
                    effectiveOutputStreams.put(outputStream.getId(), convertOutputStreamColumns(selector.getSelectionList()));
                } else {
                    LOG.warn("Unhandled execution element: {}", executionElement.toString());
                }
            }
            // Set effective input streams
            policyExecutionPlan.setInputStreams(effectiveInputStreams);

            // Set effective output streams
            policyExecutionPlan.setOutputStreams(effectiveOutputStreams);

            // Set Partitions
            for (String streamId : effectiveInputStreams.keySet()) {
                // Use shuffle partition by default
                if (!effectivePartitions.containsKey(streamId)) {
                    StreamPartition shufflePartition = new StreamPartition();
                    shufflePartition.setStreamId(streamId);
                    shufflePartition.setType(StreamPartition.Type.SHUFFLE);
                    effectivePartitions.put(streamId, shufflePartition);
                }
            }
            policyExecutionPlan.setStreamPartitions(new ArrayList<>(effectivePartitions.values()));
        } catch (Exception ex) {
            LOG.error("Got error to parse policy execution plan: \n{}", this.executionPlan, ex);
            throw ex;
        }
        return policyExecutionPlan;
    }

    private void collectStreamReferenceIdMapping(StateElement stateElement) {
        if (stateElement instanceof LogicalStateElement) {
            collectStreamReferenceIdMapping(((LogicalStateElement) stateElement).getStreamStateElement1());
            collectStreamReferenceIdMapping(((LogicalStateElement) stateElement).getStreamStateElement2());
        } else if (stateElement instanceof CountStateElement) {
            collectStreamReferenceIdMapping(((CountStateElement) stateElement).getStreamStateElement());
        } else if (stateElement instanceof EveryStateElement) {
            collectStreamReferenceIdMapping(((EveryStateElement) stateElement).getStateElement());
        } else if (stateElement instanceof NextStateElement) {
            collectStreamReferenceIdMapping(((NextStateElement) stateElement).getStateElement());
            collectStreamReferenceIdMapping(((NextStateElement) stateElement).getNextStateElement());
        } else if (stateElement instanceof StreamStateElement) {
            BasicSingleInputStream basicSingleInputStream = ((StreamStateElement) stateElement).getBasicSingleInputStream();
            this.effectiveInputStreamsAlias.put(basicSingleInputStream.getStreamReferenceId(), basicSingleInputStream.getStreamId());
        }
    }

    private String retrieveStreamId(Variable variable, Map<String, List<StreamColumn>> streamMap, Map<String, SingleInputStream> aliasMap) {
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

    private StreamPartition findStreamPartition(SingleInputStream inputStream, Selector selector) {
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

    private void retrievePartition(StreamPartition partition) {
        if (partition == null) {
            return;
        }

        if (!effectivePartitions.containsKey(partition.getStreamId())) {
            effectivePartitions.put(partition.getStreamId(), partition);
        } else if (!effectivePartitions.get(partition.getStreamId()).equals(partition)) {
            StreamPartition existingPartition = effectivePartitions.get(partition.getStreamId());
            // If same Type & Columns but different sort spec, then use larger
            if (existingPartition.getType().equals(partition.getType())
                    && ListUtils.isEqualList(existingPartition.getColumns(), partition.getColumns())
                    && partition.getSortSpec() == null) {
                LOG.info("use exist StreamPartition {}", existingPartition);
            } else if (existingPartition.getType().equals(partition.getType())
                && ListUtils.isEqualList(existingPartition.getColumns(), partition.getColumns())
                && partition.getSortSpec().getWindowPeriodMillis() > existingPartition.getSortSpec().getWindowPeriodMillis()
                || existingPartition.getType().equals(StreamPartition.Type.SHUFFLE)) {
                effectivePartitions.put(partition.getStreamId(), partition);
            } else {
                // Throw exception as it unable to conflict effectivePartitions on same stream will not be able to run in distributed mode
                throw new ExecutionPlanValidationException("You have incompatible partitions on stream " + partition.getStreamId()
                    + ": [1] " + effectivePartitions.get(partition.getStreamId()).toString() + " [2] " + partition.toString() + "");
            }
        }
    }

    private void retrieveAliasForQuery(SingleInputStream inputStream, Map<String, SingleInputStream> aliasStreamMapping) {
        if (inputStream.getStreamReferenceId() != null) {
            if (aliasStreamMapping.containsKey(inputStream.getStreamReferenceId())) {
                throw new ExecutionPlanValidationException("Duplicated stream alias " + inputStream.getStreamId() + " -> " + inputStream);
            } else {
                aliasStreamMapping.put(inputStream.getStreamReferenceId(), inputStream);
            }
        }
    }

    private StreamPartition generatePartition(String streamId, List<Window> windows, List<Variable> groupBy) {
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

    private static List<StreamColumn> convertOutputStreamColumns(List<OutputAttribute> outputAttributeList) {
        return outputAttributeList.stream().map(outputAttribute -> {
            StreamColumn streamColumn = new StreamColumn();
            streamColumn.setName(outputAttribute.getRename());
            streamColumn.setDescription(outputAttribute.getExpression().toString());
            return streamColumn;
        }).collect(Collectors.toList());
    }
}
