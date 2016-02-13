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

import java.util.Properties;

import com.typesafe.config.Config;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * The producer is thread safe and sharing a single producer instance across threads will generally be faster than having multiple instances. 
 */
public enum KafkaProducerSingleton {
	INSTANCE;	

	public KafkaProducer<String, Object>  getProducer(Config config) throws Exception{
		Properties configMap = new Properties();
		configMap.put("bootstrap.servers", NotificationPluginUtils.getPropValue(config, "kafka_broker"));
		configMap.put("metadata.broker.list", NotificationPluginUtils.getPropValue(config, "kafka_broker"));
		configMap.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configMap.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configMap.put("request.required.acks", "1");	     
		configMap.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		configMap.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		KafkaProducer<String, Object> producer = new KafkaProducer<>(configMap);
		return producer;
	}
	
}
