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
package org.apache.eagle.alert.policy;

import java.util.Map;

import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.dataproc.core.ValuesArray;

public interface PolicyEvaluator {
	/**
	 * take input and evaluate expression
	 * input has 3 fields, first is siddhiAlertContext, second one is streamName, the third is map of attribute name/value
	 * @param input
	 * @throws Exception
	 */
	public void evaluate(ValuesArray input) throws Exception;
	
	/**
	 * notify policy evaluator that policy is updated
	 */
	public void onPolicyUpdate(AlertDefinitionAPIEntity newAlertDef);
	
	/**
	 * notify policy evaluator that policy is deleted, here is cleanup work for this policy evaluator
	 */
	public void onPolicyDelete();
	
	/**
	 * get additional context
	 */	
	public Map<String, String> getAdditionalContext();
}
