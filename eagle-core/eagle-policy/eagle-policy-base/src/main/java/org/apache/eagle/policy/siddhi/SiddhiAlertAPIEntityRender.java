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
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.apache.eagle.policy.ResultRender;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.common.UrlBuilder;
import org.apache.eagle.policy.entity.AlertAPIEntity;
import org.apache.eagle.policy.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.policy.entity.AlertStreamSchemaEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SiddhiAlertAPIEntityRender implements ResultRender<AlertDefinitionAPIEntity, AlertAPIEntity> {

	public static final Logger LOG = LoggerFactory.getLogger(SiddhiAlertAPIEntityRender.class);
	public static final String source = ManagementFactory.getRuntimeMXBean().getName();

	@Override
	@SuppressWarnings("unchecked")
	public AlertAPIEntity render(Config config, List<String> rets, PolicyEvaluationContext<AlertDefinitionAPIEntity, AlertAPIEntity> siddhiAlertContext, long timestamp) {
		SiddhiPolicyEvaluator<AlertDefinitionAPIEntity, AlertAPIEntity> evaluator = (SiddhiPolicyEvaluator<AlertDefinitionAPIEntity, AlertAPIEntity>) siddhiAlertContext.evaluator;
		String alertExecutorId = siddhiAlertContext.alertExecutor.getExecutorId();
		AlertAPIEntity entity = new AlertAPIEntity();
		AlertContext context = new AlertContext();
		String sourceStreams = evaluator.getAdditionalContext().get(Constants.SOURCE_STREAMS);
		String[] sourceStreamsArr = sourceStreams.split(",");		
		List<String> attrRenameList = evaluator.getOutputStreamAttrNameList();		
		Map<String, String> tags = new HashMap<String, String>();
		for (String sourceStream : sourceStreamsArr) {
			 List<AlertStreamSchemaEntity> list = StreamMetadataManager.getInstance().getMetadataEntitiesForStream(sourceStream.trim());
			 for (AlertStreamSchemaEntity alertStream : list) {
				 if (alertStream.getUsedAsTag() != null && alertStream.getUsedAsTag() == true) {
					 String attrName = alertStream.getTags().get(Constants.ATTR_NAME);
					 tags.put(attrName, rets.get(attrRenameList.indexOf(attrName)));
				 }				 
			 }			 
		}

		for (int index = 0; index < rets.size(); index++) {
			//attrRenameList.get(0) -> "eagleAlertContext". We need to skip "eagleAlertContext", index is from 1 for attRenameList.
			context.addProperty(attrRenameList.get(index + 1), rets.get(index));
		}

		StringBuilder sb = new StringBuilder();
		for (Entry<String, String> entry : context.getProperties().entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			sb.append(key + "=\"" + value + "\" ");			
		}
		context.addAll(evaluator.getAdditionalContext());
		String policyId = context.getProperty(Constants.POLICY_ID);
		String alertMessage = "The Policy \"" + policyId + "\" has been detected with the below information: " + sb.toString() ;
		String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
		String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
		String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		Integer port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		context.addProperty(Constants.ALERT_EVENT, sb.toString());
		context.addProperty(Constants.ALERT_MESSAGE, alertMessage);
		context.addProperty(Constants.ALERT_TIMESTAMP_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));
		context.addProperty(EagleConfigConstants.DATA_SOURCE, dataSource);
		context.addProperty(EagleConfigConstants.SITE, site);
		entity.setTimestamp(timestamp);
		/** If we need to add severity tag, we should add severity filed in AbstractpolicyDefinition, and pass it down **/
		tags.put(EagleConfigConstants.SITE, site);
		tags.put(EagleConfigConstants.DATA_SOURCE, dataSource);
		tags.put(Constants.SOURCE_STREAMS, context.getProperty(Constants.SOURCE_STREAMS));
		tags.put(Constants.POLICY_ID, context.getProperty(Constants.POLICY_ID));
		tags.put(Constants.ALERT_SOURCE, source);
		tags.put(Constants.ALERT_EXECUTOR_ID, alertExecutorId);
		entity.setTags(tags);

		context.addProperty(Constants.POLICY_DETAIL_URL, UrlBuilder.buiildPolicyDetailUrl(host, port, tags));
		context.addProperty(Constants.ALERT_DETAIL_URL, UrlBuilder.buildAlertDetailUrl(host, port, entity));
		entity.setAlertContext(context);
		return entity;
	}	
}
