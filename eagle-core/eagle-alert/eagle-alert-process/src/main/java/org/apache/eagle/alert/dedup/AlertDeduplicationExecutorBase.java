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
package org.apache.eagle.alert.dedup;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.config.DeduplicatorConfig;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.DynamicPolicyLoader;
import org.apache.eagle.alert.policy.PolicyLifecycleMethods;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AlertDeduplicationExecutorBase extends JavaStormStreamExecutor2<String, AlertAPIEntity> implements PolicyLifecycleMethods {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AlertDeduplicationExecutorBase.class);
	protected Config config;
	protected DEDUP_TYPE dedupType;

	private List<String> alertExecutorIdList;
	private volatile CopyOnWriteHashMap<String, DefaultDeduplicator<AlertAPIEntity>> alertDedups;
	private AlertDefinitionDAO dao;

	public enum DEDUP_TYPE {
		ENTITY,
		EMAIL
	}

	public AlertDeduplicationExecutorBase(List<String> alertExecutorIdList, DEDUP_TYPE dedupType, AlertDefinitionDAO dao){
		this.alertExecutorIdList = alertExecutorIdList;
		this.dedupType = dedupType;
		this.dao = dao;
	}
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;
	}
	
	public DefaultDeduplicator<AlertAPIEntity> createAlertDedup(AlertDefinitionAPIEntity alertDef) {
		DeduplicatorConfig dedupConfig = null;
		try {
			dedupConfig = JsonSerDeserUtils.deserialize(alertDef.getDedupeDef(), DeduplicatorConfig.class);
		}
		catch (Exception ex) {
			LOG.warn("Initial dedupConfig error, " + ex.getMessage());
		}

        if (dedupConfig != null) {
			if (dedupType.equals(DEDUP_TYPE.ENTITY)) {
				return new DefaultDeduplicator<>(dedupConfig.getAlertDedupIntervalMin());
			} else if (dedupType.equals(DEDUP_TYPE.EMAIL)) {
				return new DefaultDeduplicator<>(dedupConfig.getEmailDedupIntervalMin());
			}
		}

		return null;
	}
	
	@Override
	public void init() {		
        String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
        String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
	    Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs;	    	    
	    try {
	 		initialAlertDefs = dao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
	    }
	    catch (Exception ex) {
 			LOG.error("fail to initialize initialAlertDefs: ", ex);
	        throw new IllegalStateException("fail to initialize initialAlertDefs: ", ex);
        }
	    Map<String, DefaultDeduplicator<AlertAPIEntity>> tmpDeduplicators = new HashMap<String, DefaultDeduplicator<AlertAPIEntity>>();
        if(initialAlertDefs == null || initialAlertDefs.isEmpty()){
            LOG.warn("No alert definitions was found for site: "+site+", dataSource: "+dataSource);
        } else {
		    for (String alertExecutorId: alertExecutorIdList) {
			    if(initialAlertDefs.containsKey(alertExecutorId)){
                    for(AlertDefinitionAPIEntity alertDef : initialAlertDefs.get(alertExecutorId).values()){
                       try {
                          DefaultDeduplicator<AlertAPIEntity> deduplicator = createAlertDedup(alertDef);
                          if (deduplicator != null)
                              tmpDeduplicators.put(alertDef.getTags().get(AlertConstants.POLICY_ID), deduplicator);
                          else LOG.warn("The dedup interval is not set, alertDef: " + alertDef);
                        }
                        catch (Throwable t) {
                            LOG.error("Got an exception when initial dedup config, probably dedup config is not set: " + t.getMessage() + "," + alertDef);
                        }
                    }
                } else {
                    LOG.info(String.format("No alert definitions found for site: %s, dataSource: %s, alertExecutorId: %s",site,dataSource,alertExecutorId));
                }
		    }
        }

		alertDedups = new CopyOnWriteHashMap<>();
		alertDedups.putAll(tmpDeduplicators);
		DynamicPolicyLoader policyLoader = DynamicPolicyLoader.getInstance();
		policyLoader.init(initialAlertDefs, dao, config);
		for (String alertExecutorId : alertExecutorIdList) {
		 	policyLoader.addPolicyChangeListener(alertExecutorId, this);
		}
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, AlertAPIEntity>> outputCollector){
        String policyId = (String) input.get(0);
        AlertAPIEntity alertEntity = (AlertAPIEntity) input.get(1);
        DefaultDeduplicator<AlertAPIEntity> dedup;
        synchronized(alertDedups) {
            dedup = alertDedups.get(policyId);
        }

        List<AlertAPIEntity> ret = Arrays.asList(alertEntity);
        if (dedup == null) {
            LOG.warn("Dedup config for policyId " + policyId + " is not set or is not a valid config");
        } else {
            if (dedup.getDedupIntervalMin() == -1) {
                LOG.warn("the dedup interval is set as -1, which mean all alerts should be deduped(skipped)");
                return;
            }
            ret = dedup.dedup(ret);
        }
        for (AlertAPIEntity entity : ret) {
            outputCollector.collect(new Tuple2(policyId, entity));
        }
    }

	public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
		if(LOG.isDebugEnabled()) LOG.debug("Alert dedup config to be added : " + added);
		for(AlertDefinitionAPIEntity alertDef : added.values()){
			LOG.info("Alert dedup config really added " + alertDef);
			DefaultDeduplicator<AlertAPIEntity> dedup = createAlertDedup(alertDef);
			if (dedup != null) {
				synchronized(alertDedups) {		
					alertDedups.put(alertDef.getTags().get(AlertConstants.POLICY_ID), dedup);
				}
			}
		}
	}
	
	public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
		LOG.info("Alert dedup config changed : " + changed);
		for(AlertDefinitionAPIEntity alertDef : changed.values()){
			LOG.info("Alert dedup config really changed " + alertDef);
			DefaultDeduplicator<AlertAPIEntity> dedup = createAlertDedup(alertDef);
			if (dedup != null) {
				synchronized(alertDedups) {
					alertDedups.put(alertDef.getTags().get(AlertConstants.POLICY_ID), dedup);
				}
			}
		}
	}
	
	public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
		LOG.info("alert dedup config deleted : " + deleted);
		for(AlertDefinitionAPIEntity alertDef : deleted.values()){
			LOG.info("alert dedup config deleted " + alertDef);
			// no cleanup to do, just remove it
			synchronized(alertDedups) {		
				alertDedups.remove(alertDef.getTags().get(AlertConstants.POLICY_ID));
			}
		}
	}
}
