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
	private Map<String, AlertEmailGenerator> emailGenerators = new ConcurrentHashMap<>();
	private final static int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
	private final static int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
	private final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute
	private transient ThreadPoolExecutor executorPool;
	private NotificationStatus status = new NotificationStatus();

	@Override
	public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
		executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		LOG.info(" Creating Email Generator... ");
		for( AlertDefinitionAPIEntity  entity : initAlertDefs ){
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			for( Map<String,String> notificationConfigMap : configMaps ){
				String notificationType = notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE);
				// for backward compatibility, default notification is email
				if(notificationType == null || notificationType.equalsIgnoreCase(NotificationConstants.EMAIL_NOTIFICATION)){
					AlertEmailGenerator generator = createEmailGenerator(notificationConfigMap);
						this.emailGenerators.put(entity.getTags().get(Constants.POLICY_ID), generator);
						LOG.info("Successfully initialized email notification for policy " + entity.getTags().get(Constants.POLICY_ID) + ",with " + notificationConfigMap);
				}
			}
		}
	}

	/**
	 * @param notificationConf
	 * @throws Exception
     */
	@Override
	public void update(String policyId, Map<String,String> notificationConf  , boolean isPolicyDelete ) throws Exception {
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.emailGenerators.remove(policyId);
			return;
		}
		AlertEmailGenerator generator = createEmailGenerator(notificationConf);
		this.emailGenerators.put(policyId , generator );
		LOG.info("created/updated email generator for updated policy " + policyId);
	}

	/**
	 * API to send email
	 * @param alertEntity
	 * @throws Exception
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) throws  Exception {
		String policyId = alertEntity.getTags().get(Constants.POLICY_ID);
		AlertEmailGenerator generator = this.emailGenerators.get(policyId);
		boolean isSuccess = generator.sendAlertEmail(alertEntity);
		if( !isSuccess ) {
			status.errorMessage = "Failed to send email";
			status.successful = false;
		}else {
			status.errorMessage = "";
			status.successful = true;
		}
	}

	@Override
	public NotificationStatus getStatus() {
		return this.status;
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
				withEagleProps(EagleConfigFactory.load().getConfig().getObject("eagleProps")).
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