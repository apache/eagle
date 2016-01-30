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

package org.apache.eagle.notification;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.email.EmailBuilder;
import org.apache.eagle.notification.email.EmailGenerator;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  Email Notification API to send email/sms  
 */
@Resource(name = NotificationConstants.EMAIL_NOTIFICATION_RESOURCE_NM , description = "  Email Notification API to trigger email/sms ")
public class EmailNotification  implements NotificationPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(EmailNotification.class);
	private Map<String, List<EmailGenerator>> emailGenerators = new ConcurrentHashMap<String, List<EmailGenerator>>();
	private NotificationStatus status ;

	/**
	 * Initializing required objects for email notification plug in
	 * @throws Exception
     */
	public void _init() throws  Exception {
		emailGenerators.clear();
		List<AlertDefinitionAPIEntity> activeAlerts = new ArrayList<AlertDefinitionAPIEntity>();
		// find out all policies and its notification Config
		try{
			activeAlerts = NotificationPluginUtils.fetchActiveAlerts();
		}catch (Exception ex ){
			LOG.error(ex.getMessage());
			throw  new Exception(" Email Notification Cannot be initialized . Reason : "+ex.getMessage());
		}
		LOG.info(" Creating Email Generator... ");
		// Create Email Generator
		for( AlertDefinitionAPIEntity  entity : activeAlerts ){
			// for each plugin
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			for( Map<String,String> notificationConfigMap : configMaps ){
				// single policy can have multiple configs , only load Email Notifications
				if(notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE).equalsIgnoreCase(NotificationConstants.EMAIL_NOTIFICATION_RESOURCE_NM)){
					List<EmailGenerator> tmpList = createEmailGenerator(notificationConfigMap);
					this.emailGenerators.put(entity.getTags().get(Constants.POLICY_ID) , tmpList);
				}
			} // end of for
		}
	}

	/**
	 * Update - API to update policy delete/create/update in Notification Plug-ins
	 * @param notificationConf
	 * @throws Exception
     */
	@Override
	public void update( Map<String,String> notificationConf  , boolean isPolicyDelete ) throws Exception {
		// TODO: check if its delete request
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.emailGenerators.remove(notificationConf.get(Constants.POLICY_ID));
			return;
		}
		List<EmailGenerator> tmpList = createEmailGenerator(notificationConf);
		this.emailGenerators.put(notificationConf.get(Constants.POLICY_ID) , tmpList );
	}

	/**
	 * API to send email/sms
	 * @param alertEntity
	 * @throws Exception
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) throws  Exception {
		status = new NotificationStatus();
		String policyId = alertEntity.getTags().get(Constants.POLICY_ID);
		System.out.println(" Email Notification ");
		List<EmailGenerator> generatorList = this.emailGenerators.get(policyId);
		boolean isSuccess = false;
		for( EmailGenerator gen : generatorList ) {
			isSuccess = gen.sendAlertEmail(alertEntity);
			if( !isSuccess ) {
				status.setMessage(" Failed to send email ");
				status.setNotificationSuccess(false);
			}else
				status.setNotificationSuccess(true);
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
	public List<EmailGenerator> createEmailGenerator( Map<String,String> notificationConfig ) {
		List<EmailGenerator> gens = new ArrayList<EmailGenerator>();
		if (notificationConfig == null) {
			return gens;
		}
		String tplFileName = notificationConfig.get(NotificationConstants.TPL_FILE_NAME);
		if (tplFileName == null || tplFileName.equals("")) {
			tplFileName = "ALERT_DEFAULT.vm";
		}
		EmailGenerator gen = EmailBuilder.newBuilder().
				withEagleProps(EagleConfigFactory.load().getConfig().getObject("eagleProps")).
				withSubject(notificationConfig.get(NotificationConstants.SUBJECT)).
				withSender(notificationConfig.get(NotificationConstants.SENDER)).
				withRecipients(notificationConfig.get(NotificationConstants.RECIPIENTS)).
				withTplFile(tplFileName).build();
		gens.add(gen);
		return gens;
	}
}