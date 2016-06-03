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
import org.apache.eagle.notification.base.NotificationStatus;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  send alert to Kafka bus
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AlertKafkaPlugin implements NotificationPlugin {
	private static final Logger LOG = LoggerFactory.getLogger(AlertKafkaPlugin.class);
	private List<NotificationStatus> statusList = new Vector<>();
	private Map<String, List<Map<String, String>>> kafaConfigs = new ConcurrentHashMap<>();
	private Config config;

	@Override
	public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
		this.config = config;
		for( AlertDefinitionAPIEntity entity : initAlertDefs ) {
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			this.update(entity.getTags().get(Constants.POLICY_ID), configMaps, false);
		}
	}

	/**
	 * Update API to update policy delete/create/update in Notification Plug-ins
	 * @param  notificationConfigCollection
	 * @param isPolicyDelete
	 * @throws Exception
     */
	@Override
	public void update(String policyId, List<Map<String,String>> notificationConfigCollection, boolean isPolicyDelete ) throws Exception {
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.kafaConfigs.remove(policyId);
			return;
		}
		Vector<Map<String, String>> kafkaConfigList = new Vector<>();
		for(Map<String,String> notificationConfigMap : notificationConfigCollection){
			String notificationType = notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE);
			if(notificationType == null){
				LOG.error("invalid notificationType for this notification, ignoring and continue " + notificationConfigMap);
				continue;
			}else {
				// single policy can have multiple configs , only load Kafka Config's
				if (notificationType.equalsIgnoreCase(NotificationConstants.KAFKA_STORE)) {
					kafkaConfigList.add(notificationConfigMap);
				}
			}
		}
		if(kafkaConfigList.size() != 0) {
			kafaConfigs.put(policyId, kafkaConfigList);
		}
	}

	/**
	 * Post Notification to KafkaTopic
	 * @param alertEntity
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) {
		String policyId = alertEntity.getTags().get(Constants.POLICY_ID);
		for(Map<String, String> kafkaConfig: this.kafaConfigs.get(policyId)) {
			NotificationStatus status = new NotificationStatus();
			try{
				KafkaProducer producer = KafkaProducerSingleton.INSTANCE.getProducer(kafkaConfig);
				producer.send(createRecord(alertEntity, kafkaConfig.get(NotificationConstants.TOPIC)));
				status.successful = true;
				status.errorMessage = "";
			}catch(Exception ex ){
				LOG.error("fail writing alert to Kafka bus", ex);
				status.successful = false;
				status.errorMessage = ex.getMessage();
			}
			this.statusList.add(status);
		}
	}

	/**
	 * To Create  KafkaProducer Record 
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	private ProducerRecord  createRecord(AlertAPIEntity entity, String topic) throws Exception {
		ProducerRecord  record  = new ProducerRecord(topic, NotificationPluginUtils.objectToStr(entity));
		return record;
	}	
	
	@Override
	public List<NotificationStatus> getStatusList() {
		return statusList;
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
