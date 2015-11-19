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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.executor.AlertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

public class SiddhiQueryCallbackImpl extends QueryCallback{

	private SiddhiPolicyEvaluator evaluator;
	public static final Logger LOG = LoggerFactory.getLogger(SiddhiQueryCallbackImpl.class);
	public static final ObjectMapper mapper = new ObjectMapper();	
	public Config config;
	
	public SiddhiQueryCallbackImpl(Config config, SiddhiPolicyEvaluator evaluator) {
		this.config = config;		
		this.evaluator = evaluator;
	}
	
	public List<String> getOutputMessage(Event event) {
		Object[] data = event.getData();
		List<String> rets = new ArrayList<String>();
		boolean isFirst = true;
		for (Object object : data) {
			// The first field is siddhiAlertContext, skip it
			if (isFirst) {
				isFirst = false;
				continue;
			}
			String value = null;
			if (object instanceof Double) {
				value = String.valueOf((Double)object);
			}
			else if (object instanceof Integer) {
				value = String.valueOf((Integer)object);
			}
			else if (object instanceof Long) {
				value = String.valueOf((Long)object);
			}
			else if (object instanceof String) {
				value = (String)object;
			}
			else if (object instanceof Boolean) {
				value = String.valueOf((Boolean)object);
			}
			rets.add(value);
		}
		return rets;
	}
	
	@Override
	public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
		Object[] data = inEvents[0].getData();
		EagleAlertContext siddhiAlertContext = (EagleAlertContext)data[0];
		List<String> rets = getOutputMessage(inEvents[0]);
		AlertAPIEntity alert = SiddhiAlertAPIEntityRendner.render(config, rets, siddhiAlertContext, timeStamp);
		AlertExecutor alertExecutor = siddhiAlertContext.alertExecutor;
		alertExecutor.onAlerts(siddhiAlertContext, Arrays.asList(alert));
	}
}
