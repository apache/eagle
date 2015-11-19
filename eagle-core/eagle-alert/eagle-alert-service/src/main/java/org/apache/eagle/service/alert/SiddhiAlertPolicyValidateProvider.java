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
package org.apache.eagle.service.alert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.siddhi.AttributeType;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.common.DateTimeUtil;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class SiddhiAlertPolicyValidateProvider extends AlertPolicyValidateProvider{

	public String type;
	public List<String> streamNames;
	public String policyDefinition;	
	public static Logger LOG = LoggerFactory.getLogger(PolicyValidateResource.class);
	public static final String EXECUTION_PLAN_NAME = "query";
	
	@SuppressWarnings({"unchecked"})
	public String getStreamDef(String streamName) {
		GenericEntityServiceResource resource = new GenericEntityServiceResource();
		String startTime = "1969-01-01 00:00:00";
		String endTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(Long.MAX_VALUE);
		int pageSize = 1000;
		String query = AlertConstants.ALERT_STREAM_SCHEMA_SERVICE_ENDPOINT_NAME + "[@streamName=\"" + streamName + "\"]{*}";
		GenericServiceAPIResponseEntity<AlertStreamSchemaEntity> streamResponse = resource.search(query, startTime, endTime, pageSize, null, false, false, 0L, 0, true, 0, null, false);
		List<AlertStreamSchemaEntity> list = streamResponse.getObj();
		
		Map<String, String> map = new HashMap<String, String>();
		for(AlertStreamSchemaEntity entity : list){
			map.put(entity.getTags().get("attrName"), entity.getAttrType());
		}
		StringBuilder sb = new StringBuilder();
		sb.append("dataobj object,");
		for(Map.Entry<String, String> entry : map.entrySet()){
			String attrName = entry.getKey();
			sb.append(attrName);
			sb.append(" ");
			String attrType = entry.getValue();
			if(attrType.equalsIgnoreCase(AttributeType.STRING.name())){
				sb.append("string");
			}else if(attrType.equalsIgnoreCase(AttributeType.INTEGER.name())){
				sb.append("int");
			}else if(attrType.equalsIgnoreCase(AttributeType.LONG.name())){
				sb.append("long");
			}else if(attrType.equalsIgnoreCase(AttributeType.BOOL.name())){
				sb.append("bool");
			}else{
				LOG.error("AttrType is not recognized, ignore : " + attrType);
			}
			sb.append(",");
		}
		if(sb.length() > 0){
			sb.deleteCharAt(sb.length()-1);
		}
		
		String siddhiStreamDefFormat = "define stream " + streamName + " (" + "%s" + ");";
		String streamDef = String.format(siddhiStreamDefFormat, sb.toString());
		return streamDef;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public GenericServiceAPIResponseEntity validate() {
		GenericServiceAPIResponseEntity result = new GenericServiceAPIResponseEntity();
		SiddhiManager siddhiManager = new SiddhiManager();
		ExecutionPlanRuntime executionPlanRuntime = null;
		try {				
			String streamDefs = new String();
			for(String streamName : streamNames){
				//String streamDef = SiddhiStreamMetadataUtils.convertToStreamDef(streamName);
				//We don't use SiddhiStreamMetadataUtils, for it only consume one dataSource
				String streamDef = getStreamDef(streamName);
				LOG.info("Siddhi stream definition : " + streamDef);
				streamDefs += streamDef;
			}		
			
			String executionPlan = streamDefs + " @info(name = '" + EXECUTION_PLAN_NAME + "') " +  policyDefinition;
			executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
		}
		catch (Exception ex) {
			result.setSuccess(false);
			result.setException(ex);
			return result;
		}
		finally {
			if (executionPlanRuntime != null) {
				executionPlanRuntime.shutdown();
			}
		}
		result.setSuccess(true);
		return result;
	}
	
	@Override
	public String PolicyType() {
		return "siddhiCEPEngine";
	}
	
	@Override
	public List<Module> BindingModules() {
		Module module = new SimpleModule("policyValidate").registerSubtypes(new NamedType(SiddhiAlertPolicyValidateProvider.class, PolicyType()));
		return Arrays.asList(module);	
	}
}
