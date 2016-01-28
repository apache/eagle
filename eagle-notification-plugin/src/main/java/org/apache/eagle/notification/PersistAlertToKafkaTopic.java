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
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  Notification Implementation to persist events into KafkaBus  
 */

@Resource(name = "Kafka Store" , description = "Persist Alert Entity to Kafka")
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PersistAlertToKafkaTopic  implements NotificationPlugin {

	private NotificationStatus status;

	private static final Logger LOG = LoggerFactory.getLogger(PersistAlertToKafkaTopic.class);
	private Map<String, AlertDefinitionAPIEntity> activeAlerts = new ConcurrentHashMap<String, AlertDefinitionAPIEntity>();
	private Map<String, KafkaTopicConfig> kafaConfigs = new ConcurrentHashMap<String, KafkaTopicConfig>();
	private Config config;
	private PolicyDefinitionDAO policyDefinitionDao;

	@Override
	public void _init() throws  Exception  {
		config = EagleConfigFactory.load().getConfig();
		String site = config.getString("eagleProps.site");
		String dataSource = config.getString("eagleProps.dataSource");
		activeAlerts.clear();
		kafaConfigs.clear();
		policyDefinitionDao = new PolicyDefinitionEntityDAOImpl(new EagleServiceConnector(config) , Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME);
		try{
			activeAlerts = policyDefinitionDao.findActiveAlertDefsByNotification( site , dataSource ,"Kafka Store");
		}catch (Exception ex ){
			LOG.error(ex.getMessage());
			throw  new Exception(" Kafka Store  Cannot be initialized . Reason : "+ex.getMessage());
		}
		Set<String> policies = activeAlerts.keySet();
		for( String policyId : policies )
		{
			Module module = new SimpleModule("notification").registerSubtypes(new NamedType(KafkaTopicConfig.class, "kafka topic"));
			KafkaTopicConfig kafkaConfig = new KafkaTopicConfig();
			try {
				kafkaConfig = JsonSerDeserUtils.deserialize(activeAlerts.get(policyId).getNotificationDef(), KafkaTopicConfig.class, Arrays.asList(module));
				this.kafaConfigs.put(policyId,kafkaConfig);
			}catch (Exception ex){
				LOG.error(" Exception when initializing PersistAlertToKafkaTopic. Reason : "+ex.getMessage());
			}
		}
	}

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
		ProducerRecord  record  = new ProducerRecord( this.kafaConfigs.get(policyId).getKafkaTopic(), entity.toString());
		return record;
	}	
	
	@Override
	public NotificationStatus getStatus() {		
		return status;
	}
}
