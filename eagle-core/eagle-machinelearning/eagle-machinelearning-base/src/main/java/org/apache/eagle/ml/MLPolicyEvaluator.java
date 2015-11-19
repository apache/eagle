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
package org.apache.eagle.ml;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.config.AbstractPolicyDefinition;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.PolicyEvaluator;
import org.apache.eagle.alert.policy.PolicyManager;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.ml.impl.MLAnomalyCallbackImpl;
import org.apache.eagle.ml.model.MLAlgorithm;
import org.apache.eagle.ml.model.MLPolicyDefinition;
import org.apache.eagle.ml.utils.MLReflectionUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MLPolicyEvaluator implements PolicyEvaluator {
	private static Logger LOG = LoggerFactory.getLogger(MLPolicyEvaluator.class);
    private final String[] sourceStreams;
    private volatile MLRuntime mlRuntime;
	private String policyName;
	private Config config;
	private Map<String,String> context;

	private class MLRuntime{
		MLPolicyDefinition mlPolicyDef;
		MLAlgorithmEvaluator[] mlAlgorithmEvaluators;
		List<MLAnomalyCallback> mlAnomalyCallbacks = new ArrayList<>();
	}

	public MLPolicyEvaluator(Config config, String policyName, AbstractPolicyDefinition policyDef, String[] sourceStreams){
		this(config, policyName, policyDef, sourceStreams, false);
	}

	/**
	 * needValidation does not take effect for machine learning use case
	 * @param policyDef
	 * @param sourceStreams
	 * @param needValidation
	 */
	public MLPolicyEvaluator(Config config, String policyName, AbstractPolicyDefinition policyDef, String[] sourceStreams, boolean needValidation){
		this.config = config;
        this.policyName = policyName;
        this.sourceStreams = sourceStreams;
        LOG.info("Initializing policy named: "+policyName);
        this.context = new HashMap<>();
        this.context.put(AlertConstants.SOURCE_STREAMS, StringUtils.join(sourceStreams,","));
		this.init(policyDef);
	}

	public void init(AbstractPolicyDefinition policyDef){
		LOG.info("Initializing MLPolicyEvaluator ...");
		try{
			mlRuntime = newMLRuntime((MLPolicyDefinition) policyDef);
		}catch(Exception e){
			LOG.error("ML Runtime creation failed: " + e.getMessage());
		}
	}

	private MLRuntime newMLRuntime(MLPolicyDefinition mlPolicyDef) {
		MLRuntime runtime = new MLRuntime();

		try{
			runtime.mlPolicyDef = mlPolicyDef;

			LOG.info("policydef: " + ((runtime.mlPolicyDef == null)? "policy definition is null": "policy definition is not null"));
			Properties alertContext = runtime.mlPolicyDef.getContext();
			LOG.info("alert context received null? " + ((alertContext == null? "yes": "no")));
			MLAnomalyCallback callbackImpl = new MLAnomalyCallbackImpl(this, config);
			runtime.mlAnomalyCallbacks.add(callbackImpl);

			MLAlgorithm[] mlAlgorithms = mlPolicyDef.getAlgorithms();
			runtime.mlAlgorithmEvaluators = new MLAlgorithmEvaluator[mlAlgorithms.length];
			LOG.info("mlAlgorithms size:: " + mlAlgorithms.length);
			int i = 0; 
			for(MLAlgorithm algorithm:mlAlgorithms){
                MLAlgorithmEvaluator mlAlgorithmEvaluator = MLReflectionUtils.newMLAlgorithmEvaluator(algorithm);
                mlAlgorithmEvaluator.init(algorithm,config);
				runtime.mlAlgorithmEvaluators[i] =  mlAlgorithmEvaluator;
				LOG.info("mlAlgorithmEvaluator: " + mlAlgorithmEvaluator.toString());
						mlAlgorithmEvaluator.register(callbackImpl);
				i++;
			}
		}catch(Exception ex){
            LOG.error("Failed to create runtime for policy named: "+this.getPolicyName(),ex);
		}
		return runtime;
	}
	
	@Override
	public void evaluate(ValuesArray data) throws Exception {
		LOG.info("Evaluate called with input: " + data.size());
		synchronized(mlRuntime){
			for(MLAlgorithmEvaluator mlAlgorithm:mlRuntime.mlAlgorithmEvaluators){
				mlAlgorithm.evaluate(data);
			}
		}
	}

	@Override
	public void onPolicyUpdate(AlertDefinitionAPIEntity newAlertDef) {
		LOG.info("onPolicyUpdate called");
		AbstractPolicyDefinition policyDef = null;		
		try {
			policyDef = JsonSerDeserUtils.deserialize(newAlertDef.getPolicyDef(), 
					AbstractPolicyDefinition.class, PolicyManager.getInstance().getPolicyModules(newAlertDef.getTags().get("policyType")));
		} catch (Exception ex) {
			LOG.error("initial policy def error, ", ex);
		}
		MLRuntime previous = mlRuntime;
		mlRuntime = newMLRuntime((MLPolicyDefinition) policyDef);
		synchronized (previous) {
			previous.mlAnomalyCallbacks = null;
			previous.mlAlgorithmEvaluators = null;
			previous.mlPolicyDef = null; 
		}
		previous = null;
	}

	@Override
	public void onPolicyDelete() {
		LOG.info("onPolicyDelete called");
		MLRuntime previous = mlRuntime;
		synchronized (previous) {
			previous.mlAnomalyCallbacks = null;
			previous.mlAlgorithmEvaluators = null;
			previous.mlPolicyDef = null; 
		}
		previous = null;
	}

    public String getPolicyName() {
		return policyName;
	}
	
	public Map<String, String> getAdditionalContext() {
		return this.context;
	}
	
	public List<String> getOutputStreamAttrNameList() {
		return new ArrayList<String>();
	}
}