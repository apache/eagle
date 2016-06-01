package org.apache.eagle.alert.engine.evaluator;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

import backtype.storm.metric.api.MultiCountMetric;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class PolicyHandlerContext {
    private PolicyDefinition policyDefinition;
    private PolicyGroupEvaluator parentEvaluator;
    private MultiCountMetric policyCounter;
    private String policyEvaluatorId;

    public PolicyDefinition getPolicyDefinition() {
        return policyDefinition;
    }

    public void setPolicyDefinition(PolicyDefinition policyDefinition) {
        this.policyDefinition = policyDefinition;
    }

    public PolicyGroupEvaluator getParentEvaluator() {
        return parentEvaluator;
    }

    public void setParentEvaluator(PolicyGroupEvaluator parentEvaluator) {
        this.parentEvaluator = parentEvaluator;
    }

    public void setPolicyCounter(MultiCountMetric metric) {
        this.policyCounter = metric;
    }

    public MultiCountMetric getPolicyCounter() {
        return policyCounter;
    }

    public String getPolicyEvaluatorId() {
        return policyEvaluatorId;
    }

    public void setPolicyEvaluatorId(String policyEvaluatorId) {
        this.policyEvaluatorId = policyEvaluatorId;
    }
}