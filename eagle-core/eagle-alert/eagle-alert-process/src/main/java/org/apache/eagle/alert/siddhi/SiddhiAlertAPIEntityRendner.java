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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.notification.UrlBuilder;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.metric.AlertContext;
import com.typesafe.config.Config;

public class SiddhiAlertAPIEntityRendner {

	public static final Logger LOG = LoggerFactory.getLogger(SiddhiAlertAPIEntityRendner.class);
	public static final String source = ManagementFactory.getRuntimeMXBean().getName();
	
	public static AlertAPIEntity render(Config config, List<String> rets, EagleAlertContext siddhiAlertContext, long timestamp) {
		SiddhiPolicyEvaluator evaluator = (SiddhiPolicyEvaluator)siddhiAlertContext.evaluator;
		String alertExecutorId = siddhiAlertContext.alertExecutor.getAlertExecutorId();
		AlertAPIEntity entity = new AlertAPIEntity();
		AlertContext context = new AlertContext();
		String sourceStreams = evaluator.getAdditionalContext().get(AlertConstants.SOURCE_STREAMS);
		String[] sourceStreamsArr = sourceStreams.split(",");		
		List<String> attrRenameList = evaluator.getOutputStreamAttrNameList();		
		Map<String, String> tags = new HashMap<String, String>();
		for (String sourceStream : sourceStreamsArr) {
			 List<AlertStreamSchemaEntity> list = StreamMetadataManager.getInstance().getMetadataEntitiesForStream(sourceStream.trim());
			 for (AlertStreamSchemaEntity alertStream : list) {
				 if (alertStream.getUsedAsTag() != null && alertStream.getUsedAsTag() == true) {
					 String attrName = alertStream.getTags().get(AlertConstants.ATTR_NAME);
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
		String policyId = context.getProperty(AlertConstants.POLICY_ID); 
		String alertMessage = "The Policy \"" + policyId + "\" has been detected with the below information: " + sb.toString() ;
		String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
		String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
		String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		Integer port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		context.addProperty(AlertConstants.ALERT_EVENT, sb.toString());
		context.addProperty(AlertConstants.ALERT_MESSAGE, alertMessage);
		context.addProperty(AlertConstants.ALERT_TIMESTAMP_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));
		context.addProperty(EagleConfigConstants.DATA_SOURCE, dataSource);
		context.addProperty(EagleConfigConstants.SITE, site);
		entity.setTimestamp(timestamp);
		/** If we need to add severity tag, we should add severity filed in AbstractpolicyDefinition, and pass it down **/
		tags.put(EagleConfigConstants.SITE, site);
		tags.put(EagleConfigConstants.DATA_SOURCE, dataSource);
		tags.put(AlertConstants.SOURCE_STREAMS, context.getProperty(AlertConstants.SOURCE_STREAMS));		
		tags.put(AlertConstants.POLICY_ID, context.getProperty(AlertConstants.POLICY_ID));		
		tags.put(AlertConstants.ALERT_SOURCE, source);
		tags.put(AlertConstants.ALERT_EXECUTOR_ID, alertExecutorId);
		entity.setTags(tags);

		context.addProperty(AlertConstants.POLICY_DETAIL_URL, UrlBuilder.buiildPolicyDetailUrl(host, port, tags));
		context.addProperty(AlertConstants.ALERT_DETAIL_URL, UrlBuilder.buildAlertDetailUrl(host, port, entity));
		entity.setAlertContext(context);
		return entity;
	}	
}
