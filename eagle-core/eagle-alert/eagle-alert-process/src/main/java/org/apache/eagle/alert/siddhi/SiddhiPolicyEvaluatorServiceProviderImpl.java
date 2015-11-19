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

import java.util.Arrays;
import java.util.List;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.policy.PolicyEvaluator;
import org.apache.eagle.alert.policy.PolicyEvaluatorServiceProvider;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class SiddhiPolicyEvaluatorServiceProviderImpl implements PolicyEvaluatorServiceProvider {
	@Override
	public String getPolicyType() {
		return AlertConstants.policyType.siddhiCEPEngine.name();
	}

	@Override
	public Class<? extends PolicyEvaluator> getPolicyEvaluator() {
		return SiddhiPolicyEvaluator.class;
	}

	@Override
	public List<Module> getBindingModules() {
		Module module1 = new SimpleModule(AlertConstants.POLICY_DEFINITION).registerSubtypes(new NamedType(SiddhiPolicyDefinition.class, getPolicyType()));
		return Arrays.asList(module1);
	}
}
