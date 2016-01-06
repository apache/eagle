/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.executor;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.metric.reportor.EagleCounterMetric;
import org.apache.eagle.metric.reportor.EagleMetricListener;
import org.apache.eagle.metric.reportor.EagleServiceReporterMetricListener;
import org.apache.eagle.metric.reportor.MetricKeyCodeDecoder;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to report policy evaluation statistics
 */
public class PolicyStatsReporterImpl implements PolicyStatsReporter{
    private	static long MERITE_GRANULARITY = DateUtils.MILLIS_PER_MINUTE;
    private Map<String, Map<String, String>> dimensionsMap; // cache it for performance
    private Map<String, String> baseDimensions;
    private EagleMetricListener listener;

    private MetricRegistry registry;

    @Override
    public void incrIncomingEvent() {
        updateCounter(EAGLE_EVENT_COUNT, baseDimensions);
    }

    @Override
    public void incrPolicyEvaluation(String policyId) {
        updateCounter(EAGLE_POLICY_EVAL_COUNT, getDimensions(policyId));
    }

    @Override
    public void incrPolicyEvaluationFailure(String policyId) {
        updateCounter(EAGLE_POLICY_EVAL_FAIL_COUNT, getDimensions(policyId));
    }

    @Override
    public void incrAlert(String policyId, int occurrences) {
        updateCounter(EAGLE_ALERT_COUNT, getDimensions(policyId), occurrences);
    }

    public PolicyStatsReporterImpl(Config config, String alertExecutorId, int partitionSeq){
        String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        int port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

        String username = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) ?
                config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) : null;
        String password = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) ?
                config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) : null;

        //TODO: need to it replace it with batch flush listener
        registry = new MetricRegistry();
        listener = new EagleServiceReporterMetricListener(host, port, username, password);

        baseDimensions = new HashMap<>();
        baseDimensions.put(AlertConstants.ALERT_EXECUTOR_ID, alertExecutorId);
        baseDimensions.put(AlertConstants.PARTITIONSEQ, String.valueOf(partitionSeq));
        baseDimensions.put(AlertConstants.SOURCE, ManagementFactory.getRuntimeMXBean().getName());
        baseDimensions.put(EagleConfigConstants.DATA_SOURCE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE));
        baseDimensions.put(EagleConfigConstants.SITE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE));
        dimensionsMap = new HashMap<>();
    }

    private void updateCounter(String name, Map<String, String> dimensions, double value) {
        long current = System.currentTimeMillis();
        String metricName = MetricKeyCodeDecoder.codeMetricKey(name, dimensions);
        if (registry.getMetrics().get(metricName) == null) {
            EagleCounterMetric metric = new EagleCounterMetric(current, metricName, value, MERITE_GRANULARITY);
            metric.registerListener(listener);
            registry.register(metricName, metric);
        } else {
            EagleCounterMetric metric = (EagleCounterMetric) registry.getMetrics().get(metricName);
            metric.update(value, current);
            //TODO: need remove unused metric from registry
        }
    }

    private void updateCounter(String name, Map<String, String> dimensions) {
        updateCounter(name, dimensions, 1.0);
    }

    private Map<String, String> getDimensions(String policyId) {
        if (dimensionsMap.get(policyId) == null) {
            Map<String, String> newDimensions = new HashMap<String, String>(baseDimensions);
            newDimensions.put(AlertConstants.POLICY_ID, policyId);
            dimensionsMap.put(policyId, newDimensions);
        }
        return dimensionsMap.get(policyId);
    }
}
