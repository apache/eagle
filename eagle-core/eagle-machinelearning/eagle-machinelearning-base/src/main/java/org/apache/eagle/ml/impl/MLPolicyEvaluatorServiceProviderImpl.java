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

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.policy.PolicyEvaluator;
import org.apache.eagle.alert.policy.PolicyEvaluatorServiceProvider;
import org.apache.eagle.ml.MLPolicyEvaluator;
import org.apache.eagle.ml.model.MLPolicyDefinition;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MLPolicyEvaluatorServiceProviderImpl implements PolicyEvaluatorServiceProvider {

	private final static String ALERT_CONTEXT = "alertContext";

	@Override
	public String getPolicyType() {
		return AlertConstants.policyType.MachineLearning.name();
	}

	@Override
	public Class<? extends PolicyEvaluator> getPolicyEvaluator() {
		return MLPolicyEvaluator.class;
	}

	@Override
	public List<Module> getBindingModules() {
		Module module1 = new SimpleModule(AlertConstants.POLICY_DEFINITION).registerSubtypes(new NamedType(MLPolicyDefinition.class, getPolicyType()));
		Module module2 = new SimpleModule(ALERT_CONTEXT).registerSubtypes(new NamedType(Properties.class, getPolicyType()));
		return Arrays.asList(module1, module2);
	}
}