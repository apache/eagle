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
package eagle.ml.impl;

import eagle.alert.common.AlertConstants;
import eagle.alert.entity.AlertAPIEntity;
import eagle.alert.siddhi.EagleAlertContext;
import eagle.common.DateTimeUtil;
import eagle.ml.MLAnomalyCallback;
import eagle.ml.MLPolicyEvaluator;
import eagle.ml.model.MLCallbackResult;
import eagle.common.metric.AlertContext;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
	public void receive(MLCallbackResult aResult,EagleAlertContext alertContext) {
		LOG.info("Receive called with : " + aResult.toString());
        AlertAPIEntity alert = renderAlert(aResult,alertContext);
        alertContext.alertExecutor.onAlerts(alertContext, Arrays.asList(alert));
	}

    private AlertAPIEntity renderAlert(MLCallbackResult aResult,EagleAlertContext alertContext){
        String site = config.getString("eagleProps.site");
        String dataSource = config.getString("eagleProps.dataSource");

        AlertAPIEntity entity = new AlertAPIEntity();
        entity.setDescription(aResult.toString());

        Map<String, String> tags = new HashMap<>();
        tags.put(AlertConstants.SITE_TAG, site);
        tags.put(AlertConstants.DATA_SOURCE, dataSource);
        tags.put(AlertConstants.SOURCE_STREAMS, alertContext.evaluator.getAdditionalContext().get(AlertConstants.SOURCE_STREAMS));
        tags.put(AlertConstants.POLICY_ID, alertContext.policyId);
        tags.put(AlertConstants.ALERT_SOURCE, source);
        tags.put(AlertConstants.ALERT_EXECUTOR_ID, alertContext.alertExecutor.getAlertExecutorId());
        entity.setTags(tags);

        entity.setTimestamp(aResult.getTimestamp());

        AlertContext context = new AlertContext();

        if(aResult.getContext() != null) context.addAll(aResult.getContext());

        String alertMessage = "Anomaly activities detected by algorithm ["+aResult.getAlgorithmName()+"] with information: " + aResult.toString() ;
        context.addProperty(AlertConstants.ALERT_EVENT, aResult.toString());
        context.addProperty(AlertConstants.ALERT_MESSAGE, alertMessage);
        context.addProperty(AlertConstants.ALERT_TIMESTAMP_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));

        try {
            site = config.getString("eagleProps.site");
            dataSource = config.getString("eagleProps.dataSource");
            context.addProperty(AlertConstants.DATA_SOURCE, dataSource);
            context.addProperty(AlertConstants.SITE_TAG, site);
        } catch (Exception ex) {
            LOG.error("site, dataSource not set in config file, ", ex);
        }

        context.addProperty(AlertConstants.DATA_SOURCE, dataSource);
        context.addProperty(AlertConstants.SITE_TAG, site);
        context.addProperty(AlertConstants.POLICY_NAME, alertContext.policyId);

        entity.setAlertContext(context);
        return entity;
    }
}