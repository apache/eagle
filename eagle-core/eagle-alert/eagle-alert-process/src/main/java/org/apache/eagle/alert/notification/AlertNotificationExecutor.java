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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.eagle.notification.plugin.NewNotificationPluginManagerImpl;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.policy.DynamicPolicyLoader;
import org.apache.eagle.policy.PolicyLifecycleMethods;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor1;
import org.apache.eagle.datastream.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * notify alert by email, kafka message, storage or other means
 */
public class AlertNotificationExecutor extends JavaStormStreamExecutor1<String> implements PolicyLifecycleMethods<AlertDefinitionAPIEntity> {
	private static final long serialVersionUID = 1690354365435407034L;
	private static final Logger LOG = LoggerFactory.getLogger(AlertNotificationExecutor.class);
	private Config config;
	/** Notification Manager - Responsible for forward and invoke configured Notification Plugin **/
	private NewNotificationPluginManagerImpl notificationManager;

	private List<String> alertExecutorIdList;
	private PolicyDefinitionDAO dao;


    public AlertNotificationExecutor(List<String> alertExecutorIdList, PolicyDefinitionDAO dao){
		this.alertExecutorIdList = alertExecutorIdList;
		this.dao = dao;
	}

	@Override
	public void init() {
		String site = config.getString("eagleProps.site");
		String dataSource = config.getString("eagleProps.dataSource");
		Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs;
		try {
			initialAlertDefs = dao.findActivePoliciesGroupbyExecutorId( site, dataSource );
		}
		catch (Exception ex) {
			LOG.error("fail to initialize initialAlertDefs: ", ex);
			throw new IllegalStateException("fail to initialize initialAlertDefs: ", ex);
		}

		if(initialAlertDefs == null || initialAlertDefs.isEmpty()){
			LOG.warn("No alert definitions found for site: "+site+", dataSource: "+dataSource);
		}
		try{
			notificationManager = new NewNotificationPluginManagerImpl(config);
		}catch (Exception ex ){
			LOG.error("Fail to initialize NotificationManager: ", ex);
			throw new IllegalStateException("Fail to initialize NotificationManager: ", ex);
		}

		DynamicPolicyLoader<AlertDefinitionAPIEntity> policyLoader = DynamicPolicyLoader.getInstanceOf(AlertDefinitionAPIEntity.class);
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
		AlertAPIEntity alertEntity = (AlertAPIEntity) input.get(1);
		processAlerts(Arrays.asList(alertEntity));
	}

	private void processAlerts(List<AlertAPIEntity> list) {
		for (AlertAPIEntity entity : list) {
			notificationManager.notifyAlert(entity);
		}
	}

	@Override
	public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
		if(LOG.isDebugEnabled()) LOG.debug(" alert notification config changed : " + added);
		for(AlertDefinitionAPIEntity alertDef : added.values()){
			LOG.info("alert notification config really changed " + alertDef);
			notificationManager.updateNotificationPlugins( alertDef , false );
		}
	}

	@Override
	public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
		if(LOG.isDebugEnabled()) LOG.debug("alert notification config to be added : " + changed);
		for(AlertDefinitionAPIEntity alertDef : changed.values()){
			LOG.info("alert notification config really added " + alertDef);
			notificationManager.updateNotificationPlugins( alertDef , false );
		}
	}

	@Override
	public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
		if(LOG.isDebugEnabled()) LOG.debug("alert notification config to be deleted : " + deleted);
		for(AlertDefinitionAPIEntity alertDef : deleted.values()){
			LOG.info("alert notification config really deleted " + alertDef);
			notificationManager.updateNotificationPlugins( alertDef , true );
		}
	}
}