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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import com.netflix.config.AbstractPollingScheduler;
import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.DynamicConfiguration;
import com.netflix.config.FixedDelayPollingScheduler;
import com.netflix.config.PollListener;
import com.netflix.config.PollResult;
import com.netflix.config.PolledConfigurationSource;
import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;

public class DynamicPolicyLoader {
	private static final Logger LOG = LoggerFactory.getLogger(DynamicPolicyLoader.class);
	
	private final int defaultInitialDelayMillis = 30*1000;
	private final int defaultDelayMillis = 60*1000;
	private final boolean defaultIgnoreDeleteFromSource = true;
	private volatile CopyOnWriteHashMap<String, List<PolicyLifecycleMethods>> policyChangeListeners = new CopyOnWriteHashMap<String, List<PolicyLifecycleMethods>>();
	private static DynamicPolicyLoader instance = new DynamicPolicyLoader();
	private volatile boolean initialized = false;
	
	public void addPolicyChangeListener(String alertExecutorId, PolicyLifecycleMethods alertExecutor){
		synchronized(policyChangeListeners) {
			if (policyChangeListeners.get(alertExecutorId) == null) {
				policyChangeListeners.put(alertExecutorId, new ArrayList<PolicyLifecycleMethods>());
			}
			policyChangeListeners.get(alertExecutorId).add(alertExecutor);
		}
	}
	
	public static DynamicPolicyLoader getInstance(){
		return instance;
	}
	
	/**
	 * singleton with init would be good for unit test as well, and it ensures that
	 * initialization happens only once before you use it.  
	 * @param config
	 * @param dao
	 */
	public void init(Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs, 
			AlertDefinitionDAO dao, Config config){
		if(!initialized){
			synchronized(this){
				if(!initialized){
					internalInit(initialAlertDefs, dao, config);
					initialized = true;
				}
			}
		}
	}
	
	/**
	 * map from alertExecutorId+partitionId to AlertExecutor which implements PolicyLifecycleMethods
	 * @param initialAlertDefs
	 * @param dao
	 * @param config
	 */
	private void internalInit(Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs,
			AlertDefinitionDAO dao, Config config){
		if(!config.getBoolean("dynamicConfigSource.enabled")) {
            return;
        }
		AbstractPollingScheduler scheduler = new FixedDelayPollingScheduler(
                config.getInt("dynamicConfigSource.initDelayMillis"),
                config.getInt("dynamicConfigSource.delayMillis"),
                false
        );

		scheduler.addPollListener(new PollListener(){
			@Override
			public void handleEvent(EventType eventType, PollResult lastResult,
					Throwable exception) {
				if (lastResult == null) {
					LOG.error("The lastResult is null, something must be wrong, probably the eagle service is dead!");
					throw new RuntimeException("The lastResult is null, probably the eagle service is dead! ", exception);
				}
				Map<String, Object> added = lastResult.getAdded();
				Map<String, Object> changed = lastResult.getChanged();
				Map<String, Object> deleted = lastResult.getDeleted();
				for(Map.Entry<String, List<PolicyLifecycleMethods>> entry : policyChangeListeners.entrySet()){
					String alertExecutorId = entry.getKey();
					for (PolicyLifecycleMethods policyLifecycleMethod : entry.getValue()) {
						Map<String, AlertDefinitionAPIEntity> addedPolicies = (Map<String, AlertDefinitionAPIEntity>)added.get(trimPartitionNum(alertExecutorId));
						if(addedPolicies != null && addedPolicies.size() > 0){
							policyLifecycleMethod.onPolicyCreated(addedPolicies);
						}
						Map<String, AlertDefinitionAPIEntity> changedPolicies = (Map<String, AlertDefinitionAPIEntity>)changed.get(trimPartitionNum(alertExecutorId));
						if(changedPolicies != null && changedPolicies.size() > 0){
							policyLifecycleMethod.onPolicyChanged(changedPolicies);
						}
						Map<String, AlertDefinitionAPIEntity> deletedPolicies = (Map<String, AlertDefinitionAPIEntity>)deleted.get(trimPartitionNum(alertExecutorId));
						if(deletedPolicies != null && deletedPolicies.size() > 0){
							policyLifecycleMethod.onPolicyDeleted(deletedPolicies);
						}
					}
				}
			}
			private String trimPartitionNum(String alertExecutorId){
				int i = alertExecutorId.lastIndexOf('_');
				if(i != -1){
					return alertExecutorId.substring(0, i);
				}
				return alertExecutorId;
			}
		});
		
		ConcurrentCompositeConfiguration finalConfig = new ConcurrentCompositeConfiguration();
		      
		PolledConfigurationSource source = new DynamicPolicySource(initialAlertDefs, dao, config);

		try{
			DynamicConfiguration dbSourcedConfiguration = new DynamicConfiguration(source, scheduler);
			finalConfig.addConfiguration(dbSourcedConfiguration);
		}catch(Exception ex){
			LOG.warn("Fail loading from DB, continue without DB sourced configuration", ex);
		}
	}
	
	public static class DynamicPolicySource implements PolledConfigurationSource{
		private static Logger LOG = LoggerFactory.getLogger(DynamicPolicySource.class);
		private Config config;
		private AlertDefinitionDAO dao;
		/**
		 * mapping from alertExecutorId to list of policies 
		 */
		private Map<String, Map<String, AlertDefinitionAPIEntity>> cachedAlertDefs;
		
		public DynamicPolicySource(Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs, AlertDefinitionDAO dao, Config config){
			this.cachedAlertDefs = initialAlertDefs;
			this.dao = dao;
			this.config = config;
		}

		public PollResult poll(boolean initial, Object checkPoint) throws Exception {
			LOG.info("Poll policy from eagle service " +  config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST) +
					":" + config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT) );
			Map<String, Map<String, AlertDefinitionAPIEntity>> newAlertDefs = 
					dao.findActiveAlertDefsGroupbyAlertExecutorId(config.getString("eagleProps.site"),
                            config.getString("eagleProps.dataSource"));
			
			// compare runtime alertDefs with cachedAlertDefs and figure out what are added/deleted/updated
			Map<String, Object> added = new HashMap<String, Object>();
			Map<String, Object> changed = new HashMap<String, Object>();
			Map<String, Object> deleted = new HashMap<String, Object>();
			
			Set<String> newAlertExecutorIds = newAlertDefs.keySet();
			Set<String> cachedAlertExecutorIds = cachedAlertDefs.keySet();
			
			// dynamically adding new alert executor is not supported, because alert executor is pre-built while program starts up
			Collection<String> addedAlertExecutorIds = CollectionUtils.subtract(newAlertExecutorIds, cachedAlertExecutorIds);
			if(addedAlertExecutorIds != null && addedAlertExecutorIds.size() > 0){
				LOG.warn("New alertExecutorIds are found : " + addedAlertExecutorIds);
			}
			
			// if one alert executor is missing, it means all policy under that alert executor should be removed
			Collection<String> deletedAlertExecutorIds = CollectionUtils.subtract(cachedAlertExecutorIds, newAlertExecutorIds);
			if(deletedAlertExecutorIds != null && deletedAlertExecutorIds.size() > 0){
				LOG.warn("Some alertExecutorIds are deleted : " + deletedAlertExecutorIds);
				for(String deletedAlertExecutorId : deletedAlertExecutorIds){
					deleted.put(deletedAlertExecutorId, cachedAlertDefs.get(deletedAlertExecutorId));
				}
			}
			
			// we need calculate added/updated/deleted policy for all executors which are not deleted
//			Collection<String> updatedAlertExecutorIds = CollectionUtils.intersection(newAlertExecutorIds, cachedAlertExecutorIds);
            Collection<String> updatedAlertExecutorIds = newAlertExecutorIds;
			for(String updatedAlertExecutorId : updatedAlertExecutorIds){
				Map<String, AlertDefinitionAPIEntity> newPolicies = newAlertDefs.get(updatedAlertExecutorId);
				Map<String, AlertDefinitionAPIEntity> cachedPolicies = cachedAlertDefs.get(updatedAlertExecutorId);
				PolicyComparator.compare(updatedAlertExecutorId, newPolicies, cachedPolicies, added, changed, deleted);
			}
			
			cachedAlertDefs = newAlertDefs;
			return PollResult.createIncremental(added, changed, deleted, new Date().getTime());
		}
	}
	
	public static class PolicyComparator{
		public static void compare(String alertExecutorId, Map<String, AlertDefinitionAPIEntity> newPolicies, Map<String, AlertDefinitionAPIEntity> cachedPolicies, 
				Map<String, Object> added, Map<String, Object> changed, Map<String, Object> deleted){
			Set<String> newPolicyIds = newPolicies.keySet();
            Set<String> cachedPolicyIds = cachedPolicies != null ? cachedPolicies.keySet() : new HashSet<String>();
			Collection<String> addedPolicyIds = CollectionUtils.subtract(newPolicyIds, cachedPolicyIds);
			Collection<String> deletedPolicyIds = CollectionUtils.subtract(cachedPolicyIds, newPolicyIds);
			Collection<String> changedPolicyIds = CollectionUtils.intersection(cachedPolicyIds, newPolicyIds);
			if(addedPolicyIds != null && addedPolicyIds.size() > 0){
				Map<String, AlertDefinitionAPIEntity> tmp = new HashMap<String, AlertDefinitionAPIEntity>();
				for(String addedPolicyId : addedPolicyIds){
					tmp.put(addedPolicyId, newPolicies.get(addedPolicyId));
				}
				added.put(alertExecutorId, tmp);
			}
			if(deletedPolicyIds != null && deletedPolicyIds.size() > 0){
				Map<String, AlertDefinitionAPIEntity> tmp = new HashMap<String, AlertDefinitionAPIEntity>();
				for(String deletedPolicyId : deletedPolicyIds){
					tmp.put(deletedPolicyId, cachedPolicies.get(deletedPolicyId));
				}
				deleted.put(alertExecutorId, tmp);
			}
			if(changedPolicyIds != null && changedPolicyIds.size() > 0){
				Map<String, AlertDefinitionAPIEntity> tmp = new HashMap<String, AlertDefinitionAPIEntity>();
				for(String changedPolicyId : changedPolicyIds){
					// check if policy is really changed
					if(!newPolicies.get(changedPolicyId).equals(cachedPolicies.get(changedPolicyId))){
						tmp.put(changedPolicyId, newPolicies.get(changedPolicyId));
					}
				}
				changed.put(alertExecutorId, tmp);
			}
		}
	}
}
