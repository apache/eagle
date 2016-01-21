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
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Siddhi call back implementation
 *
 * @param <T> - The policy definition type
 * @param <K> - K the alert entity type
 */
public class SiddhiQueryCallbackImpl<T extends AbstractPolicyDefinitionEntity, K> extends QueryCallback{

	private static final Logger LOG = LoggerFactory.getLogger(SiddhiQueryCallbackImpl.class);

	private final Config config;
	private final PolicyEvaluationContext<T, K> siddhiEvaluateContext;

	public SiddhiQueryCallbackImpl(Config config, PolicyEvaluationContext<T, K> siddhiContext) {
		this.config = config;
		this.siddhiEvaluateContext = siddhiContext;
	}
	
	public static List<String> convertToString(List<Object> data) {
		List<String> rets = new ArrayList<String>();
		for (Object object : data) {
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

	public static List<Object> getOutputObject(Object[] data) {
		List<Object> rets = new ArrayList<>(data.length);
//		boolean isFirst = true;
		for (Object object : data) {
//			// The first field is siddhiAlertContext, skip it
//			if (isFirst) {
//				isFirst = false;
//				continue;
//			}
			rets.add(object);
		}
		return rets;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
		List<Object> rets = getOutputObject(inEvents[0].getData());
		K alert = siddhiEvaluateContext.resultRender.render(config, rets, siddhiEvaluateContext, timeStamp);
		SiddhiEvaluationHandler<T, K> handler = siddhiEvaluateContext.alertExecutor;
		handler.onEvalEvents(siddhiEvaluateContext, Arrays.asList(alert));
	}
}
