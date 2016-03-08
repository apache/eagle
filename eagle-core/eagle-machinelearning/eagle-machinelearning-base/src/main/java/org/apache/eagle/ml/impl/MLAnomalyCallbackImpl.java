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
package org.apache.eagle.ml.impl;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.ml.MLAnomalyCallback;
import org.apache.eagle.ml.MLPolicyEvaluator;
import org.apache.eagle.ml.model.MLCallbackResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class MLAnomalyCallbackImpl implements MLAnomalyCallback {
	private static Logger LOG = LoggerFactory.getLogger(MLAnomalyCallbackImpl.class);
	private MLPolicyEvaluator mlAlertEvaluator;
	private Config config;


    public static final String source = ManagementFactory.getRuntimeMXBean().getName();
	
	public MLAnomalyCallbackImpl(MLPolicyEvaluator mlAlertEvaluator, Config config){
		this.mlAlertEvaluator = mlAlertEvaluator;
		this.config = config;
	}

    /**
     * TODO: generate alert
     *
     * @param aResult
     * @param alertContext context
     */
	@Override
	public void receive(MLCallbackResult aResult,PolicyEvaluationContext alertContext) {
		LOG.info("Receive called with : " + aResult.toString());
        AlertAPIEntity alert = renderAlert(aResult,alertContext);
        alertContext.alertExecutor.onEvalEvents(alertContext, Arrays.asList(alert));
	}

    private AlertAPIEntity renderAlert(MLCallbackResult aResult,PolicyEvaluationContext alertContext){
        String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
        String applicatioin = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.APPLICATION);

        AlertAPIEntity entity = new AlertAPIEntity();
        entity.setDescription(aResult.toString());

        Map<String, String> tags = new HashMap<>();
        tags.put(EagleConfigConstants.SITE, site);
        tags.put(EagleConfigConstants.APPLICATION, applicatioin);
        tags.put(Constants.SOURCE_STREAMS, (String)alertContext.evaluator.getAdditionalContext().get(Constants.SOURCE_STREAMS));
        tags.put(Constants.POLICY_ID, alertContext.policyId);
        tags.put(Constants.ALERT_SOURCE, source);
        tags.put(Constants.ALERT_EXECUTOR_ID, alertContext.alertExecutor.getExecutorId());
        entity.setTags(tags);

        entity.setTimestamp(aResult.getTimestamp());

        AlertContext context = new AlertContext();

        if(aResult.getContext() != null) context.addAll(aResult.getContext());

        String alertMessage = "Anomaly activities detected by algorithm ["+aResult.getAlgorithmName()+"] with information: " + aResult.toString() ;
        context.addProperty(Constants.ALERT_EVENT, aResult.toString());
        context.addProperty(Constants.ALERT_MESSAGE, alertMessage);
        context.addProperty(Constants.ALERT_TIMESTAMP_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));

        try {
            site = config.getString("eagleProps.site");
            applicatioin = config.getString("eagleProps.application");
            context.addProperty(EagleConfigConstants.APPLICATION, applicatioin);
            context.addProperty(EagleConfigConstants.SITE, site);
        } catch (Exception ex) {
            LOG.error("site, dataSource not set in config file, ", ex);
        }

        context.addProperty(EagleConfigConstants.APPLICATION, applicatioin);
        context.addProperty(EagleConfigConstants.SITE, site);
        context.addProperty(Constants.POLICY_NAME, alertContext.policyId);

        entity.setAlertContext(context);
        return entity;
    }
}