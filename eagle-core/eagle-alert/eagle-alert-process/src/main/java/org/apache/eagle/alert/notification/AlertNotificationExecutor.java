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
package org.apache.eagle.alert.notification;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;
import org.apache.eagle.alert.config.EmailNotificationConfig;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.DynamicPolicyLoader;
import org.apache.eagle.alert.policy.PolicyLifecycleMethods;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor1;
import org.apache.eagle.datastream.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * notify alert by email, sms or other means
 * currently we only implements email notification
 */
public class AlertNotificationExecutor extends JavaStormStreamExecutor1<String> implements PolicyLifecycleMethods {

	private static final long serialVersionUID = 1690354365435407034L;
	private static final Logger LOG = LoggerFactory.getLogger(AlertNotificationExecutor.class);
	private Config config;

	private List<String> alertExecutorIdList;
	private volatile CopyOnWriteHashMap<String, List<AlertEmailGenerator>> alertEmailGeneratorsMap;
	private AlertDefinitionDAO dao;

    private final static int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
    private final static int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
    private final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute

    private transient ThreadPoolExecutor executorPool;

    public AlertNotificationExecutor(List<String> alertExecutorIdList, AlertDefinitionDAO dao){
		this.alertExecutorIdList = alertExecutorIdList;
		this.dao = dao;
	}
	
	public List<AlertEmailGenerator> createAlertEmailGenerator(AlertDefinitionAPIEntity alertDef) {
		Module module = new SimpleModule("notification").registerSubtypes(new NamedType(EmailNotificationConfig.class, "email"));
		EmailNotificationConfig[] emailConfigs = new EmailNotificationConfig[0];
		try {			
			emailConfigs = JsonSerDeserUtils.deserialize(alertDef.getNotificationDef(), EmailNotificationConfig[].class, Arrays.asList(module));
		}
		catch (Exception ex) {
			LOG.warn("Initial emailConfig error, wrong format or it's error " + ex.getMessage());
		}
		List<AlertEmailGenerator> gens = new ArrayList<AlertEmailGenerator>();
		if (emailConfigs == null) {
			return gens;		
		}
		for(EmailNotificationConfig emailConfig : emailConfigs) {
			String tplFileName = emailConfig.getTplFileName();			
			if (tplFileName == null || tplFileName.equals("")) { // empty tplFileName, use default tpl file name
				tplFileName = "ALERT_DEFAULT.vm";
			}
			AlertEmailGenerator gen = AlertEmailGeneratorBuilder.newBuilder().
																withEagleProps(config.getObject("eagleProps")).
																withSubject(emailConfig.getSubject()).
																withSender(emailConfig.getSender()).
																withRecipients(emailConfig.getRecipients()).
																withTplFile(tplFileName).
                                                                withExecutorPool(executorPool).
																build();
			gens.add(gen);
		}
		return gens;
	}
	
	/**
	 * 1. register both file and database configuration
	 * 2. create email generator from configuration
	 */
    @Override
	public void init(){
        executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		Map<String, List<AlertEmailGenerator>> tmpEmailGenerators = new HashMap<String, List<AlertEmailGenerator>> ();
		
        String site = config.getString("eagleProps.site");
        String dataSource = config.getString("eagleProps.dataSource");
	    Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs;
	    try {
	 		initialAlertDefs = dao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
	    }
	    catch (Exception ex) {
 			LOG.error("fail to initialize initialAlertDefs: ", ex);
	        throw new IllegalStateException("fail to initialize initialAlertDefs: ", ex);
        }
 	   
        if(initialAlertDefs == null || initialAlertDefs.isEmpty()){
            LOG.warn("No alert definitions found for site: "+site+", dataSource: "+dataSource);
        }
        else {
		    for (String alertExecutorId: alertExecutorIdList) {
                if(initialAlertDefs.containsKey(alertExecutorId)) {
                    for (AlertDefinitionAPIEntity alertDef : initialAlertDefs.get(alertExecutorId).values()) {
                        List<AlertEmailGenerator> gens = createAlertEmailGenerator(alertDef);
                        tmpEmailGenerators.put(alertDef.getTags().get("policyId"), gens);
                    }
                }else{
                    LOG.info(String.format("No alert definitions found for site: %s, dataSource: %s, alertExecutorId: %s",site,dataSource,alertExecutorId));
                }
		    }
        }
		
		alertEmailGeneratorsMap = new CopyOnWriteHashMap<String, List<AlertEmailGenerator>>();
		alertEmailGeneratorsMap.putAll(tmpEmailGenerators);				
		DynamicPolicyLoader policyLoader = DynamicPolicyLoader.getInstance();
		policyLoader.init(initialAlertDefs, dao, config);
		for (String alertExecutorId : alertExecutorIdList) {
			policyLoader.addPolicyChangeListener(alertExecutorId, this);
		}
	}

    @Override
	public void prepareConfig(Config config) {
		this.config = config;
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple1<String>> outputCollector){
        String policyId = (String) input.get(0);
        AlertAPIEntity alertEntity = (AlertAPIEntity) input.get(1);
        processAlerts(policyId, Arrays.asList(alertEntity));
    }
	
	//TODO: add a thread pool for email sender?
	private void processAlerts(String policyId, List<AlertAPIEntity> list) {
		List<AlertEmailGenerator> generators;
		synchronized(alertEmailGeneratorsMap) {		
			generators = alertEmailGeneratorsMap.get(policyId);
		}
		if (generators == null) {
			LOG.warn("Notification config of policyId " + policyId + " has been deleted");
			return;
		}
		for (AlertAPIEntity entity : list) {
			for(AlertEmailGenerator generator : generators){
				generator.sendAlertEmail(entity);
			}
		}
	}

	@Override
	public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
		if(LOG.isDebugEnabled()) LOG.debug(" alert notification config changed : " + added);
		for(AlertDefinitionAPIEntity alertDef : added.values()){
			LOG.info("alert notification config really changed " + alertDef);
			List<AlertEmailGenerator> gens = createAlertEmailGenerator(alertDef);
			synchronized(alertEmailGeneratorsMap) {		
				alertEmailGeneratorsMap.put(alertDef.getTags().get("policyId"), gens);
			}
		}		
	}

	@Override
	public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
		if(LOG.isDebugEnabled()) LOG.debug("alert notification config to be added : " + changed);
		for(AlertDefinitionAPIEntity alertDef : changed.values()){
			LOG.info("alert notification config really added " + alertDef);
			List<AlertEmailGenerator> gens = createAlertEmailGenerator(alertDef);
			synchronized(alertEmailGeneratorsMap) {					
				alertEmailGeneratorsMap.put(alertDef.getTags().get("policyId"), gens);
			}
		}			
	}

	@Override
	public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
		if(LOG.isDebugEnabled()) LOG.debug("alert notification config to be deleted : " + deleted);
		for(AlertDefinitionAPIEntity alertDef : deleted.values()){
			LOG.info("alert notification config really deleted " + alertDef);
			synchronized(alertEmailGeneratorsMap) {		
				alertEmailGeneratorsMap.remove(alertDef.getTags().get("policyId"));
			}
		}		
	}
}
