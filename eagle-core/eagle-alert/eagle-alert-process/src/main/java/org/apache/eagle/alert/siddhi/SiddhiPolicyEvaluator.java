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
package org.apache.eagle.alert.siddhi;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.eagle.alert.config.AbstractPolicyDefinition;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.policy.PolicyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.selection.OutputAttribute;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.PolicyEvaluator;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import com.typesafe.config.Config;

/**
 * when policy is updated or deleted, SiddhiManager.shutdown should be invoked to release resources.
 * during this time, synchronization is important
 */
public class SiddhiPolicyEvaluator implements PolicyEvaluator{
	private final static Logger LOG = LoggerFactory.getLogger(SiddhiPolicyEvaluator.class);	
	public static final int DEFAULT_QUEUE_SIZE = 1000;
	private final BlockingQueue<AlertAPIEntity> queue = new ArrayBlockingQueue<AlertAPIEntity>(DEFAULT_QUEUE_SIZE);
	private volatile SiddhiRuntime siddhiRuntime;
	private String[] sourceStreams;
	private boolean needValidation;
	private String policyId;
	private Config config;
	private final static String EXECUTION_PLAN_NAME = "query";
	
	/**
	 * everything dependent on policyDef should be together and switched in runtime
	 */
	public static class SiddhiRuntime{
		QueryCallback callback;
		Map<String, InputHandler> siddhiInputHandlers;
		SiddhiManager siddhiManager;
		SiddhiPolicyDefinition policyDef;
		List<String> outputFields;
		String executionPlanName;
	}
	
	public SiddhiPolicyEvaluator(Config config, String policyName, AbstractPolicyDefinition policyDef, String[] sourceStreams){
		this(config, policyName, policyDef, sourceStreams, false);
	}
	
	public SiddhiPolicyEvaluator(Config config, String policyId, AbstractPolicyDefinition policyDef, String[] sourceStreams, boolean needValidation){
		this.config = config;
		this.policyId = policyId;
		this.needValidation = needValidation;
		this.sourceStreams = sourceStreams; 
		init(policyDef);
	}
	
	public void init(AbstractPolicyDefinition policyDef){			
		siddhiRuntime = createSiddhiRuntime((SiddhiPolicyDefinition)policyDef);
	}

	public static String addContextFieldIfNotExist(String expression) {		
		// select fieldA, fieldB --> select eagleAlertContext, fieldA, fieldB
		int pos = expression.indexOf("select ") + 7;
		int index = pos;
		boolean isSelectStarPattern = true;
		while(index < expression.length()) {
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

	private SiddhiRuntime createSiddhiRuntime(SiddhiPolicyDefinition policyDef){
		SiddhiManager siddhiManager = new SiddhiManager();
		Map<String, InputHandler> siddhiInputHandlers = new HashMap<String, InputHandler>();

		StringBuilder sb = new StringBuilder();		
		for(String sourceStream : sourceStreams){
			String streamDef = SiddhiStreamMetadataUtils.convertToStreamDef(sourceStream);
			LOG.info("Siddhi stream definition : " + streamDef);
			sb.append(streamDef);
		}
		
		String expression = addContextFieldIfNotExist(policyDef.getExpression());
		String executionPlan = sb.toString() + " @info(name = '" + EXECUTION_PLAN_NAME + "') " +  expression;
		ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
		
		for(String sourceStream : sourceStreams){			
			siddhiInputHandlers.put(sourceStream, executionPlanRuntime.getInputHandler(sourceStream));
		}
		executionPlanRuntime.start();

		QueryCallback callback = new SiddhiQueryCallbackImpl(config, this);		

		LOG.info("Siddhi query: " + expression);
		executionPlanRuntime.addCallback(EXECUTION_PLAN_NAME, callback);

		List<String> outputFields = new ArrayList<String>();
		try {
	        Field field = QueryCallback.class.getDeclaredField(EXECUTION_PLAN_NAME);
	        field.setAccessible(true);
	        Query query = (Query)field.get(callback);
	        List<OutputAttribute> list = query.getSelector().getSelectionList();
	        for (OutputAttribute output : list) {	        	
	        	outputFields.add(output.getRename());
	        }
		}
		catch (Exception ex) {
			LOG.error("Got an Exception when initial outputFields ", ex);
		}
		SiddhiRuntime runtime = new SiddhiRuntime();
		runtime.siddhiInputHandlers = siddhiInputHandlers;
		runtime.siddhiManager = siddhiManager;
		runtime.callback = callback;
		runtime.policyDef = policyDef;
		runtime.outputFields = outputFields;
		runtime.executionPlanName = executionPlanRuntime.getName();
		return runtime;
	}
	
	/**
	 * 1. input has 3 fields, first is siddhi context, second is streamName, the last one is map of attribute name/value
	 * 2. runtime check for input data (This is very expensive, so we ignore for now)
	 *     the size of input map should be equal to size of attributes which stream metadata defines
	 *     the attribute names should be equal to attribute names which stream metadata defines
	 *     the input field cannot be null
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void evaluate(ValuesArray data) throws Exception {
		if(LOG.isDebugEnabled()) LOG.debug("Siddhi policy evaluator consumers data :" + data);
        Object siddhiAlertContext = data.get(0);
		String streamName = (String)data.get(1);
		SortedMap map = (SortedMap)data.get(2);
		validateEventInRuntime(streamName, map);
		synchronized(siddhiRuntime){
			//insert siddhiAlertContext into the first field
			List<Object> input = new ArrayList<>();
			input.add(siddhiAlertContext);
			putAttrsIntoInputStream(input, streamName, map);
			siddhiRuntime.siddhiInputHandlers.get(streamName).send(input.toArray(new Object[0]));
		}
	}

	/**
	 * this is a heavy operation, we should avoid to use
	 * @param sourceStream
	 * @param data
	 */
	private void validateEventInRuntime(String sourceStream, SortedMap data){
		if(!needValidation)
			return;
		SortedMap<String, AlertStreamSchemaEntity> map = StreamMetadataManager.getInstance().getMetadataEntityMapForStream(sourceStream);
		if(!map.keySet().equals(data.keySet()))
			throw new IllegalStateException("incoming data schema is different from supported data schema, incoming data: " + data.keySet() + ", schema: " + map.keySet());
	}

	private void putAttrsIntoInputStream(List<Object> input, String streamName, SortedMap map) {
		if(!needValidation) {
			input.addAll(map.values());
			return;
		}
		for (Object key : map.keySet()) {
			Object value = map.get(key);
			if (value == null) {
				input.add(SiddhiStreamMetadataUtils.getAttrDefaultValue(streamName, (String)key));
			}
			else input.add(value);
		}
	}

	@Override
	public void onPolicyUpdate(AlertDefinitionAPIEntity newAlertDef) {
		AbstractPolicyDefinition policyDef = null;
		try {
			policyDef = JsonSerDeserUtils.deserialize(newAlertDef.getPolicyDef(), 
					AbstractPolicyDefinition.class, PolicyManager.getInstance().getPolicyModules(newAlertDef.getTags().get(AlertConstants.POLICY_TYPE)));
		}
		catch (Exception ex) {
			LOG.error("Initial policy def error, ", ex);
		}
		SiddhiRuntime previous = siddhiRuntime;
		siddhiRuntime = createSiddhiRuntime((SiddhiPolicyDefinition)policyDef);
		synchronized(previous){
			previous.siddhiManager.getExecutionPlanRuntime(previous.executionPlanName).shutdown();
		}
	}
	
	@Override
	public void onPolicyDelete(){
		synchronized(siddhiRuntime){
			LOG.info("Going to shutdown siddhi execution plan, planName: " + siddhiRuntime.executionPlanName);
			siddhiRuntime.siddhiManager.getExecutionPlanRuntime(siddhiRuntime.executionPlanName).shutdown();
			LOG.info("Siddhi execution plan " + siddhiRuntime.executionPlanName + " is successfully shutdown ");
		}
	}
	
	@Override
	public String toString(){
		// show the policyDef
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
		context.put(AlertConstants.SOURCE_STREAMS, sourceStreams.toString());
		context.put(AlertConstants.POLICY_ID, policyId);
		return context;
	}

	public List<String> getOutputStreamAttrNameList() {
		return siddhiRuntime.outputFields;
	}
}
