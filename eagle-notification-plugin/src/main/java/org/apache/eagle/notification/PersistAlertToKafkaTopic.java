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
import kafka.server.KafkaConfig;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.dao.PolicyDefinitionEntityDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  Notification Implementation to persist events into KafkaBus  
 */

@Resource(name = NotificationConstants.KAFKA_STORE , description = "Persist Alert Entity to Kafka")
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PersistAlertToKafkaTopic  implements NotificationPlugin {

	private NotificationStatus status;

	private static final Logger LOG = LoggerFactory.getLogger(PersistAlertToKafkaTopic.class);
	private Map<String, KafkaTopicConfig> kafaConfigs = new ConcurrentHashMap<String, KafkaTopicConfig>();

	/**
	 * Initialize all Instance required by this Plugin
	 * @throws Exception
     */
	@Override
	public void _init( Config config ) throws  Exception  {
		kafaConfigs.clear();
		List<AlertDefinitionAPIEntity> activeAlerts = null;
		try{
			activeAlerts = NotificationPluginUtils.fetchActiveAlerts();
		}catch (Exception ex ){
			LOG.error(ex.getMessage());
			throw  new Exception(" Kafka Store  Cannot be initialized . Reason : "+ex.getMessage());
		}
		for( AlertDefinitionAPIEntity entity : activeAlerts ) {
			List<Map<String,String>>  configMaps = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
			for( Map<String,String> notificationConfigMap : configMaps ){
				// single policy can have multiple configs , only load Kafka Config's
				if(notificationConfigMap.get(NotificationConstants.NOTIFICATION_TYPE).equalsIgnoreCase(NotificationConstants.KAFKA_STORE)){
					kafaConfigs.put( entity.getTags().get(Constants.POLICY_ID) ,createKafkaConfig(notificationConfigMap) );
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
	public void update( Map<String,String> notificationConf , boolean isPolicyDelete ) throws Exception {
		if( isPolicyDelete ){
			LOG.info(" Policy been deleted.. Removing reference from Notification Plugin ");
			this.kafaConfigs.remove(notificationConf.get(Constants.POLICY_ID));
			return;
		}
		kafaConfigs.put( notificationConf.get(Constants.POLICY_ID) ,createKafkaConfig(notificationConf) );
	}

	/**
	 * Post Notification to KafkaTopic
	 * @param alertEntity
     */
	@Override
	public void onAlert(AlertAPIEntity alertEntity) {
		try{
			 status = new NotificationStatus();
			processAlertEntity(alertEntity);
			status.setNotificationSuccess(true);
		}catch(Exception ex ){
			LOG.error(" Exception when Posting Alert Entity to Kafka Topic. Reason : "+ex.getMessage());
			status.setMessage(ex.getMessage());
		}		
	}

	/**
	 * Access KafkaProducer and send entity to Bus 
	 * @param alertEntity
	 * @throws Exception
	 */
	public void processAlertEntity( AlertAPIEntity alertEntity ) throws Exception {
		KafkaProducer producer = KafkaProducerSingleton.INSTANCE.getProducer();
		producer.send(createRecord(alertEntity));		
	}
	
	/**
	 * To Create  KafkaProducer Record 
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public ProducerRecord  createRecord(AlertAPIEntity entity ) throws Exception {
		String policyId = entity.getTags().get(Constants.POLICY_ID);
		ProducerRecord  record  = new ProducerRecord( this.kafaConfigs.get(policyId).getKafkaTopic(), NotificationPluginUtils.objectToStr(entity));
		return record;
	}	
	
	@Override
	public NotificationStatus getStatus() {
		return status;
	}

	/**
	 * Creates Kafka Config Object
	 * @param notificationConfig
	 * @return
     */
	private KafkaTopicConfig  createKafkaConfig( Map<String,String> notificationConfig ){
		KafkaTopicConfig kafkaConfig = new KafkaTopicConfig();
		try {
			kafkaConfig.setKafkaTopic(notificationConfig.get(NotificationConstants.KAFKA_TOPIC));
		}catch (Exception ex){
			LOG.error(" Exception when initializing PersistAlertToKafkaTopic. Reason : "+ex.getMessage());
		}
		return kafkaConfig;
	}

}
