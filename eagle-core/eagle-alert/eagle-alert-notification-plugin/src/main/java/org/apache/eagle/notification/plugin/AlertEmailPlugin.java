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
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.base.NotificationMetadata;
import org.apache.eagle.notification.base.NotificationStatus;
import org.apache.eagle.notification.email.AlertEmailGenerator;
import org.apache.eagle.notification.email.AlertEmailGeneratorBuilder;
import org.apache.eagle.notification.email.EmailBuilder;
import org.apache.eagle.notification.email.EmailGenerator;
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
public class AlertEmailPlugin implements NewNotificationPlugin {
	private static final Logger LOG = LoggerFactory.getLogger(AlertEmailPlugin.class);
	private Map<String, AlertEmailGenerator> emailGenerators = new ConcurrentHashMap<>();
	private final static int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
	private final static int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
	private final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute
	private transient ThreadPoolExecutor executorPool;
	private NotificationStatus status = new NotificationStatus();

	@Override
	public NotificationMetadata getMetadata() {
		NotificationMetadata metadata = new NotificationMetadata();
		metadata.name = NotificationConstants.EMAIL_NOTIFICATION;
		metadata.description = "Send alert to email";
		return metadata;
	}

	@Override
	public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
		executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		LOG.info(" Creating Email Generator... ");
		for( AlertDefinitionAPIEntity  entity : initAlertDefs ){
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			for( Map<String,String> notificationConfigMap : configMaps ){
				String notificationType = notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE);
				if(notificationType == null){
					LOG.error("no notificationType field for this notification, ignoring and continue " + notificationConfigMap);
					continue;
				}else {
					// single policy can have multiple configs , only load Email Notifications
					if (notificationType.equalsIgnoreCase(NotificationConstants.EMAIL_NOTIFICATION)) {
						AlertEmailGenerator generator = createEmailGenerator(notificationConfigMap);
						this.emailGenerators.put(entity.getTags().get(Constants.POLICY_ID), generator);
						LOG.info("Successfully initialized email notification for policy " + entity.getTags().get(Constants.POLICY_ID) + ",with " + notificationConfigMap);
					}
				}
			}
		}
	}

	/**
	 * @param notificationConf
	 * @throws Exception
     */
	@Override
	public void update( Map<String,String> notificationConf  , boolean isPolicyDelete ) throws Exception {
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.emailGenerators.remove(notificationConf.get(Constants.POLICY_ID));
			return;
		}
		AlertEmailGenerator generator = createEmailGenerator(notificationConf);
		this.emailGenerators.put(notificationConf.get(Constants.POLICY_ID) , generator );
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
	 * Create EmailGenerator for the given Config
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
}