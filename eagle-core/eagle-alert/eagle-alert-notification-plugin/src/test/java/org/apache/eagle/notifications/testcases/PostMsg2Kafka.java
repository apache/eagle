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

package org.apache.eagle.notifications.testcases;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.notification.plugin.KafkaProducerSingleton;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class PostMsg2Kafka {
	
	@Test
	public void postMessage2KafkaBus() throws Exception 
	{
		KafkaProducer<String, Object > producer = KafkaProducerSingleton.INSTANCE.getProducer();
		ProducerRecord<String, Object> record  = new ProducerRecord(NotificationPluginUtils.getPropValue("notification_topic_kafka"), "This is Log Message ");
		producer.send(record);
	}	
	
	@Test
	public void postAlertEntity2KafkaBus() throws Exception 
	{
		KafkaProducer<String, Object > producer = KafkaProducerSingleton.INSTANCE.getProducer();
		AlertAPIEntity entity = new AlertAPIEntity();
		entity.setDescription(" Sensitivity Path /hive/warehouse/data/customer_details ");
		entity.setRemediationID("100");
		ProducerRecord<String, Object> record  = new ProducerRecord(NotificationPluginUtils.getPropValue("notification_topic_kafka"), entity  );
		producer.send(record);
		System.out.println();
	}
}
