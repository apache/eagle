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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.Module;

public class PolicyManager {
	private final static Logger LOG = LoggerFactory.getLogger(PolicyManager.class);
	private static PolicyManager instance = new PolicyManager();

	private ServiceLoader<PolicyEvaluatorServiceProvider> loader;
	
	private Map<String, Class<? extends PolicyEvaluator>> policyEvaluators = new HashMap<String, Class<? extends PolicyEvaluator>>();
	private Map<String, List<Module>> policyModules = new HashMap<String, List<Module>>();
	
	private PolicyManager(){
		loader = ServiceLoader.load(PolicyEvaluatorServiceProvider.class);
		Iterator<PolicyEvaluatorServiceProvider> iter = loader.iterator();
		while(iter.hasNext()){
			PolicyEvaluatorServiceProvider factory = iter.next();
			LOG.info("Supported policy type : " + factory.getPolicyType());
			policyEvaluators.put(factory.getPolicyType(), factory.getPolicyEvaluator());
			policyModules.put(factory.getPolicyType(), factory.getBindingModules());
		}
	}
	
	public static PolicyManager getInstance(){
		return instance;
	}
	
	public Class<? extends PolicyEvaluator> getPolicyEvaluator(String policyType){
		return policyEvaluators.get(policyType);
	}
	
	public List<Module> getPolicyModules(String policyType){
		return policyModules.get(policyType);
	}
}
