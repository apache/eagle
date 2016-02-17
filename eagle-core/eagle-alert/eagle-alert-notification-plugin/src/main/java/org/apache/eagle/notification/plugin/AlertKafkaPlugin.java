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
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.base.NotificationMetadata;
import org.apache.eagle.notification.base.NotificationStatus;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  send alert to Kafka bus
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AlertKafkaPlugin implements NotificationPlugin {
	private static final Logger LOG = LoggerFactory.getLogger(AlertKafkaPlugin.class);
	private NotificationStatus status = new NotificationStatus();
	private Map<String, Map<String, String>> kafaConfigs = new ConcurrentHashMap<>();
	private Config config;

	@Override
	public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
		this.config = config;
		for( AlertDefinitionAPIEntity entity : initAlertDefs ) {
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			for( Map<String,String> notificationConfigMap : configMaps ){
				String notificationType = notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE);
				if(notificationType == null){
					LOG.error("no notificationType field for this notification, ignoring and continue " + notificationConfigMap);
					continue;
				}else {
					// single policy can have multiple configs , only load Kafka Config's
					if (notificationType.equalsIgnoreCase(NotificationConstants.KAFKA_STORE)) {
						kafaConfigs.put(entity.getTags().get(Constants.POLICY_ID), notificationConfigMap);
						break;
					}
				}
			}
		}
	}

	/**
	 * Update API to update policy delete/create/update in Notification Plug-ins
	 * @param  notificationConf
	 * @param isPolicyDelete
	 * @throws Exception
     */
	@Override
	public void update(String policyId, Map<String,String> notificationConf , boolean isPolicyDelete ) throws Exception {
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.kafaConfigs.remove(policyId);
			return;
		}
		kafaConfigs.put(policyId, notificationConf );
	}

	/**
	 * Post Notification to KafkaTopic
	 * @param alertEntity
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) {
		try{
			KafkaProducer producer = KafkaProducerSingleton.INSTANCE.getProducer(config);
			producer.send(createRecord(alertEntity));
			status.successful = true;
			status.errorMessage = "";
		}catch(Exception ex ){
			LOG.error("fail writing alert to Kafka bus", ex);
			status.successful = false;
			status.errorMessage = ex.getMessage();
		}
	}

	/**
	 * To Create  KafkaProducer Record 
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	private ProducerRecord  createRecord(AlertAPIEntity entity ) throws Exception {
		String policyId = entity.getTags().get(Constants.POLICY_ID);
		ProducerRecord  record  = new ProducerRecord( this.kafaConfigs.get(policyId).get("topic"), NotificationPluginUtils.objectToStr(entity));
		return record;
	}	
	
	@Override
	public NotificationStatus getStatus() {
		return status;
	}

	@Override
	public int hashCode(){
		return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
	}

	@Override
	public boolean equals(Object o){
		if(o == this)
			return true;
		if(!(o instanceof AlertKafkaPlugin))
			return false;
		return true;
	}
}
