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

import java.util.List;

import com.fasterxml.jackson.databind.Module;

/**
 * to provide extensibility, we need a clear differentiation between framework job and provider logic
 * policy evaluator framework:
 * - connect to eagle data source
 * - read all policy definitions
 * - compare with cached policy definitions
 * - figure out if policy is created, deleted or updated
 *   - if policy is created, then invoke onPolicyCreated
 *   - if policy is deleted, then invoke onPolicyDeleted
 *   - if policy is updated, then invoke onPolicyUpdated
 * - for policy update, replace old evaluator engine with new evaluator engine which is created by policy evaluator provider
 * - specify # of executors for this alert executor id
 * - dynamically balance # of policies evaluated by each alert executor
 *   - use zookeeper to balance. eaglePolicies/${alertExecutorId}/${alertExecutorInstanceId} => list of policies
 * 
 * policy evaluator business features:
 * - register mapping between policy type and PolicyEvaluator
 * - create evaluator engine runtime when configuration is changed
 *
 */
public interface PolicyEvaluatorServiceProvider {
	String getPolicyType();
	Class<? extends PolicyEvaluator> getPolicyEvaluator();
	List<Module> getBindingModules();
}
