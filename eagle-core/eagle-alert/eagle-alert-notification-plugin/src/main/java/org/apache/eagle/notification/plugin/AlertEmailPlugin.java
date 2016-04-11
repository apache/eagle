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

package org.apache.eagle.notification.plugin;

import com.typesafe.config.Config;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.base.NotificationStatus;
import org.apache.eagle.notification.email.AlertEmailGenerator;
import org.apache.eagle.notification.email.AlertEmailGeneratorBuilder;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *  Send alert to email
 */
public class AlertEmailPlugin implements NotificationPlugin {
	private static final Logger LOG = LoggerFactory.getLogger(AlertEmailPlugin.class);
	private Map<String, List<AlertEmailGenerator>> emailGenerators = new ConcurrentHashMap<>();
	private final static int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
	private final static int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
	private final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute
	private transient ThreadPoolExecutor executorPool;
	private Vector<NotificationStatus> statusList = new Vector<>();
	private Config config;

	@Override
	public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
		this.config = config;
		executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		LOG.info(" Creating Email Generator... ");
		for( AlertDefinitionAPIEntity  entity : initAlertDefs ){
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			this.update(entity.getTags().get(Constants.POLICY_ID), configMaps, false);
		}
	}

	/**
	 * @param notificationConfigCollection
	 * @throws Exception
     */
	@Override
	public void update(String policyId, List<Map<String,String>> notificationConfigCollection, boolean isPolicyDelete) throws Exception {
		if(isPolicyDelete){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.emailGenerators.remove(policyId);
			return;
		}
		Vector<AlertEmailGenerator> generators = new Vector<>();
		for(Map<String, String> notificationConf: notificationConfigCollection) {
			String notificationType = notificationConf.get(NotificationConstants.NOTIFICATION_TYPE);
			if(notificationType.equalsIgnoreCase(NotificationConstants.EMAIL_NOTIFICATION)) {
				AlertEmailGenerator generator = createEmailGenerator(notificationConf);
				generators.add(generator);
			}
		}
		if(generators.size() != 0) {
			this.emailGenerators.put(policyId, generators);
			LOG.info("created/updated email generators for policy " + policyId);
		}
	}

	/**
	 * API to send email
	 * @param alertEntity
	 * @throws Exception
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) throws  Exception {
		String policyId = alertEntity.getTags().get(Constants.POLICY_ID);
		List<AlertEmailGenerator> generators = this.emailGenerators.get(policyId);
		for(AlertEmailGenerator generator: generators) {
			boolean isSuccess = generator.sendAlertEmail(alertEntity);
			NotificationStatus status = new NotificationStatus();
			if( !isSuccess ) {
				status.errorMessage = "Failed to send email";
				status.successful = false;
			}else {
				status.errorMessage = "";
				status.successful = true;
			}
			this.statusList.add(status);
		}
	}

	@Override
	public List<NotificationStatus> getStatusList() {
		return this.statusList;
	}

	/**
	 * @param notificationConfig
	 * @return
     */
	private AlertEmailGenerator createEmailGenerator( Map<String,String> notificationConfig ) {
		String tplFileName = notificationConfig.get(NotificationConstants.TPL_FILE_NAME);
		if (tplFileName == null || tplFileName.equals("")) {
			tplFileName = "ALERT_DEFAULT.vm";
		}
		AlertEmailGenerator gen = AlertEmailGeneratorBuilder.newBuilder().
				withEagleProps(this.config.getObject("eagleProps")).
				withSubject(notificationConfig.get(NotificationConstants.SUBJECT)).
				withSender(notificationConfig.get(NotificationConstants.SENDER)).
				withRecipients(notificationConfig.get(NotificationConstants.RECIPIENTS)).
				withTplFile(tplFileName).
				withExecutorPool(this.executorPool).build();
		return gen;
	}

	@Override
	public int hashCode(){
		return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
	}

	@Override
	public boolean equals(Object o){
		if(o == this)
			return true;
		if(!(o instanceof AlertEmailPlugin))
			return false;
		return true;
	}
}