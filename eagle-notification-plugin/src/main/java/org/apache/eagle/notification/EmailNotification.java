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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.typesafe.config.Config;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.notification.email.EmailBuilder;
import org.apache.eagle.notification.email.EmailGenerator;
import org.apache.eagle.notification.email.EmailNotificationConfig;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  Email Notification API to send email/sms  
 */
@Resource(name = "Email Notification" , description = "  Email Notification API to trigger email/sms ")
public class EmailNotification  implements NotificationPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(EmailNotification.class);
	private Map<String, AlertDefinitionAPIEntity> activeAlerts = new ConcurrentHashMap<String, AlertDefinitionAPIEntity>();
	static Map<String, List<EmailGenerator>> emailGenerators = new ConcurrentHashMap<String, List<EmailGenerator>>();
	private Config config;
	private AlertDefinitionDAO alertDefinitionDao;
	private NotificationStatus status ;

	/* initialize Email Notification related Objects Properly */

	public void _init() throws  Exception {
		// Get Config Object
		config = EagleConfigFactory.load().getConfig();
		String site = config.getString("eagleProps.site");
		String dataSource = config.getString("eagleProps.dataSource");
		activeAlerts.clear();
		emailGenerators.clear();
		// find out all policies and its notification Config
		alertDefinitionDao = new AlertDefinitionDAOImpl(new EagleServiceConnector(config));
		try{
			activeAlerts = alertDefinitionDao.findActiveAlertDefsByNotification( site , dataSource ,"Email Notification");
		}catch (Exception ex ){
			LOG.error(ex.getMessage());
			throw  new Exception(" Email Notification Cannot be initialized . Reason : "+ex.getMessage());
		}
		// Create Email
		Set<String> policies = activeAlerts.keySet();
		for( String policyId : policies ){
			AlertDefinitionAPIEntity alertDef = activeAlerts.get(policyId);
			List<EmailGenerator> tmpList = createEmailGenerator(alertDef);
			this.emailGenerators.put(policyId , tmpList);
		}
	}

	@Override
	public void onAlert(AlertAPIEntity alertEntity) {
		status = new NotificationStatus();
		String policyId = alertEntity.getTags().get(AlertConstants.POLICY_ID);
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

	public List<EmailGenerator> createEmailGenerator( AlertDefinitionAPIEntity alertDef ) {

		Module module = new SimpleModule("notification").registerSubtypes(new NamedType(EmailNotificationConfig.class, "email"));
		EmailNotificationConfig[] emailConfigs = new EmailNotificationConfig[0];
		try {
			emailConfigs = JsonSerDeserUtils.deserialize(alertDef.getNotificationDef(), EmailNotificationConfig[].class, Arrays.asList(module));
		}
		catch (Exception ex) {
			LOG.warn("Initial emailConfig error, wrong format or it's error " + ex.getMessage());
		}
		List<EmailGenerator> gens = new ArrayList<EmailGenerator>();
		if (emailConfigs == null) {
			return gens;
		}
		for(EmailNotificationConfig emailConfig : emailConfigs) {
			String tplFileName = emailConfig.getTplFileName();
			if (tplFileName == null || tplFileName.equals("")) {
				tplFileName = "ALERT_DEFAULT.vm";
			}
			EmailGenerator gen = EmailBuilder.newBuilder().
					withEagleProps(config.getObject("eagleProps")).
					withSubject(emailConfig.getSubject()).
					withSender(emailConfig.getSender()).
					withRecipients(emailConfig.getRecipients()).
					withTplFile(tplFileName).build();
			gens.add(gen);
		}
		return gens;
	}
}
