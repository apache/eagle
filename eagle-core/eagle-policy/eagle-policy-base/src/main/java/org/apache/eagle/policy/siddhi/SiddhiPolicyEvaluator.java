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
package org.apache.eagle.policy.siddhi;

import com.typesafe.config.Config;
import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.apache.eagle.policy.PolicyEvaluator;
import org.apache.eagle.policy.PolicyManager;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.config.AbstractPolicyDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;

import java.lang.reflect.Field;
import java.util.*;

/**
 * when policy is updated or deleted, SiddhiManager.shutdown should be invoked to release resources.
 * during this time, synchronization is important
 */
public class SiddhiPolicyEvaluator<T extends AbstractPolicyDefinitionEntity, K> implements PolicyEvaluator<T> {

    private final static String EXECUTION_PLAN_NAME = "query";
    private final static Logger LOG = LoggerFactory.getLogger(SiddhiPolicyEvaluator.class);

    private volatile SiddhiRuntime siddhiRuntime;
    private final String[] sourceStreams;
    private final boolean needValidation;
    private final Config config;
    private final PolicyEvaluationContext<T, K> context;

    /**
     * everything dependent on policyDef should be together and switched in runtime
     */
    public static class SiddhiRuntime {
        QueryCallback queryCallback;
        Map<String, InputHandler> siddhiInputHandlers;
        SiddhiManager siddhiManager;
        SiddhiPolicyDefinition policyDef;
        List<String> outputFields;
        String executionPlanName;
    }

    public SiddhiPolicyEvaluator(Config config, PolicyEvaluationContext<T, K> context, AbstractPolicyDefinition policyDef, String[] sourceStreams) {
        this(config, context, policyDef, sourceStreams, false);
    }

    public SiddhiPolicyEvaluator(Config config, PolicyEvaluationContext<T, K> context, AbstractPolicyDefinition policyDef, String[] sourceStreams, boolean needValidation) {
        this.config = config;
        this.context = context;
        this.context.evaluator = this;
        this.needValidation = needValidation;
        this.sourceStreams = sourceStreams;
        init(policyDef);
    }

    public void init(AbstractPolicyDefinition policyDef) {
        siddhiRuntime = createSiddhiRuntime((SiddhiPolicyDefinition) policyDef);
    }

    public static String addContextFieldIfNotExist(String expression) {
        // select fieldA, fieldB --> select eagleAlertContext, fieldA, fieldB
        int pos = expression.indexOf("select ") + 7;
        int index = pos;
        boolean isSelectStarPattern = true;
        while (index < expression.length()) {
            if (expression.charAt(index) == ' ') index++;
            else if (expression.charAt(index) == '*') break;
            else {
                isSelectStarPattern = false;
                break;
            }
        }
        if (isSelectStarPattern) return expression;
        StringBuilder sb = new StringBuilder();
        sb.append(expression.substring(0, pos));
        sb.append(SiddhiStreamMetadataUtils.EAGLE_ALERT_CONTEXT_FIELD + ",");
        sb.append(expression.substring(pos, expression.length()));
        return sb.toString();
    }

    private SiddhiRuntime createSiddhiRuntime(SiddhiPolicyDefinition policyDef) {
        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, InputHandler> siddhiInputHandlers = new HashMap<String, InputHandler>();
        SiddhiRuntime runtime = new SiddhiRuntime();

        // compose execution plan sql
        String executionPlan = policyDef.getExpression();
        if (!policyDef.isContainsDefinition()) {
            StringBuilder sb = new StringBuilder();
            for (String sourceStream : sourceStreams) {
                String streamDef = SiddhiStreamMetadataUtils.convertToStreamDef(sourceStream);
                LOG.info("Siddhi stream definition : " + streamDef);
                sb.append(streamDef);
            }

            String expression = policyDef.getExpression();
            executionPlan = sb.toString() + " @info(name = '" + EXECUTION_PLAN_NAME + "') " + expression;
        }

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        for (String sourceStream : sourceStreams) {
            siddhiInputHandlers.put(sourceStream, executionPlanRuntime.getInputHandler(sourceStream));
        }
        executionPlanRuntime.start();

        LOG.info("Siddhi query: " + executionPlan);

        attachCallback(runtime, executionPlanRuntime, context);

        runtime.siddhiInputHandlers = siddhiInputHandlers;
        runtime.siddhiManager = siddhiManager;
        runtime.policyDef = policyDef;
        runtime.executionPlanName = executionPlanRuntime.getName();
        return runtime;
    }

    private void attachCallback(SiddhiRuntime runtime, ExecutionPlanRuntime executionPlanRuntime, PolicyEvaluationContext<T, K> context) {
        List<String> outputFields = new ArrayList<>();
//        String outputStreamName = config.getString("alertExecutorConfigs." + executorId + "." + "outputStream");
//        if (StringUtils.isNotEmpty(outputStreamName)) {
//            StreamCallback streamCallback = new SiddhiOutputStreamCallback<>(config, this);
//            executionPlanRuntime.addCallback(outputStreamName, streamCallback);
//            runtime.outputStreamCallback = streamCallback;
//            // find output attribute from stream call back
//            try {
//                Field field = StreamCallback.class.getDeclaredField("streamDefinition");
//                field.setAccessible(true);
//                AbstractDefinition outStreamDef = (AbstractDefinition) field.get(streamCallback);
//                outputFields = Arrays.asList(outStreamDef.getAttributeNameArray());
//            } catch (Exception ex) {
//                LOG.error("Got an Exception when initial outputFields ", ex);
//            }
//        } else {
            QueryCallback callback = new SiddhiQueryCallbackImpl<T, K>(config, context);
            executionPlanRuntime.addCallback(EXECUTION_PLAN_NAME, callback);
            runtime.queryCallback = callback;
            // find output attribute from query call back
            try {
                Field field = QueryCallback.class.getDeclaredField(EXECUTION_PLAN_NAME);
                field.setAccessible(true);
                Query query = (Query) field.get(callback);
                List<OutputAttribute> list = query.getSelector().getSelectionList();
                for (OutputAttribute output : list) {
                    outputFields.add(output.getRename());
                }
            } catch (Exception ex) {
                LOG.error("Got an Exception when initial outputFields ", ex);
            }
//        }
        runtime.outputFields = outputFields;
    }

    /**
     * 1. input has 3 fields, first is siddhi context, second is streamName, the last one is map of attribute name/value
     * 2. runtime check for input data (This is very expensive, so we ignore for now)
     * the size of input map should be equal to size of attributes which stream metadata defines
     * the attribute names should be equal to attribute names which stream metadata defines
     * the input field cannot be null
     */
    @SuppressWarnings({"rawtypes"})
    @Override
    public void evaluate(ValuesArray data) throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("Siddhi policy evaluator consumers data :" + data);
        Collector outputCollector = (Collector) data.get(0);
        String streamName = (String) data.get(1);
        SortedMap map = (SortedMap) data.get(2);
        validateEventInRuntime(streamName, map);
        synchronized (siddhiRuntime) {
            // retain the collector in the context. This assignment is idempotent
            context.outputCollector = outputCollector;

            List<Object> input = new ArrayList<>();
            putAttrsIntoInputStream(input, streamName, map);
            siddhiRuntime.siddhiInputHandlers.get(streamName).send(input.toArray(new Object[0]));
        }
    }

    /**
     * This is a heavy operation, we should avoid to use.
     * <p/>
     * This validation method will skip invalid fields in event which are not declared in stream schema otherwise it will cause exception for siddhi engine.
     *
     * @param sourceStream source steam id
     * @param data         input event
     * @see <a href="https://issues.apache.org/jira/browse/EAGLE-49">https://issues.apache.org/jira/browse/EAGLE-49</a>
     */
    private void validateEventInRuntime(String sourceStream, SortedMap data) {
        if (!needValidation)
            return;
        SortedMap<String, AlertStreamSchemaEntity> map = StreamMetadataManager.getInstance().getMetadataEntityMapForStream(sourceStream);
        if (!map.keySet().equals(data.keySet())) {
            Set<Object> badKeys = new TreeSet<>();
            for (Object key : data.keySet()) if (!map.containsKey(key)) badKeys.add(key);
            LOG.warn(String.format("Ignore invalid fields %s in event: %s from stream: %s, valid fields are: %s", badKeys.toString(), data.toString(), sourceStream, map.keySet().toString()));
            for (Object key : badKeys) data.remove(key);
        }
    }

    private void putAttrsIntoInputStream(List<Object> input, String streamName, SortedMap map) {
        if (!needValidation) {
            input.addAll(map.values());
            return;
        }
        for (Object key : map.keySet()) {
            Object value = map.get(key);
            if (value == null) {
                input.add(SiddhiStreamMetadataUtils.getAttrDefaultValue(streamName, (String) key));
            } else input.add(value);
        }
    }

    @Override
    public void onPolicyUpdate(T newAlertDef) {
        AbstractPolicyDefinition policyDef = null;
        try {
            policyDef = JsonSerDeserUtils.deserialize(newAlertDef.getPolicyDef(),
                    AbstractPolicyDefinition.class, PolicyManager.getInstance().getPolicyModules(newAlertDef.getTags().get(Constants.POLICY_TYPE)));
        } catch (Exception ex) {
            LOG.error("Initial policy def error, ", ex);
        }
        SiddhiRuntime previous = siddhiRuntime;
        siddhiRuntime = createSiddhiRuntime((SiddhiPolicyDefinition) policyDef);
        synchronized (previous) {
            previous.siddhiManager.getExecutionPlanRuntime(previous.executionPlanName).shutdown();
        }
    }

    @Override
    public void onPolicyDelete() {
        synchronized (siddhiRuntime) {
            LOG.info("Going to shutdown siddhi execution plan, planName: " + siddhiRuntime.executionPlanName);
            siddhiRuntime.siddhiManager.getExecutionPlanRuntime(siddhiRuntime.executionPlanName).shutdown();
            LOG.info("Siddhi execution plan " + siddhiRuntime.executionPlanName + " is successfully shutdown ");
        }
    }

    @Override
    public String toString() {
        return siddhiRuntime.policyDef.toString();
    }

    public String[] getStreamNames() {
        return sourceStreams;
    }

    public Map<String, String> getAdditionalContext() {
        Map<String, String> context = new HashMap<String, String>();
        StringBuilder sourceStreams = new StringBuilder();
        for (String streamName : getStreamNames()) {
            sourceStreams.append(streamName + ",");
        }
        if (sourceStreams.length() > 0) {
            sourceStreams.deleteCharAt(sourceStreams.length() - 1);
        }
        context.put(Constants.SOURCE_STREAMS, sourceStreams.toString());
        context.put(Constants.POLICY_ID, this.context.policyId);
        return context;
    }

    public List<String> getOutputStreamAttrNameList() {
        return siddhiRuntime.outputFields;
    }
}
