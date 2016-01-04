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
package org.apache.eagle.dataproc.impl.persist.druid;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.apache.eagle.dataproc.impl.persist.IPersistService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.Future;

/**
 * TODO : support more general entity input
 * @since Dec 21, 2015
 *
 */
public class DruidPersistService implements IPersistService<AggregateEntity> {

	private static final String ACKS = "acks";
	private static final String RETRIES = "retries";
	private static final String BATCH_SIZE = "batchSize";
	private static final String LINGER_MS = "lingerMs";
	private static final String BUFFER_MEMORY = "bufferMemory";
	private static final String KEY_SERIALIZER = "keySerializer";
	private static final String VALUE_SERIALIZER = "valueSerializer";
	private static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
	
	private KafkaProducer<String, AggregateEntity> producer;
	private final Config config;
	private final SortedMap<String, String> streamTopicMap;
	private final Properties props;
	
	/**
	 * <pre>
	 * props.put("bootstrap.servers", "localhost:4242");
	 * props.put("acks", "all");
	 * props.put("retries", 0);
	 * props.put("batch.size", 16384);
	 * props.put("linger.ms", 1);
	 * props.put("buffer.memory", 33554432);
	 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	 * </pre>
	 */
	public DruidPersistService(Config config) {
		this.config = config;
		Config kafkaConfig = config.getConfig("kafka");
		if (kafkaConfig == null) {
			throw new IllegalStateException("Druid persiste service failed to find kafka configurations!");
		}
		props = new Properties();
		if (kafkaConfig.hasPath(BOOTSTRAP_SERVERS)) {
			props.put(BOOTSTRAP_SERVERS, kafkaConfig.getString(BOOTSTRAP_SERVERS));
		}
		if (kafkaConfig.hasPath(ACKS)) {
			props.put(ACKS, kafkaConfig.getString(ACKS));
		}
		if (kafkaConfig.hasPath(RETRIES)) {
			props.put(RETRIES, kafkaConfig.getInt(RETRIES));
		}
		if (kafkaConfig.hasPath(BATCH_SIZE)) {
			props.put("batch.size", kafkaConfig.getInt(BATCH_SIZE));
		}
		if (kafkaConfig.hasPath(LINGER_MS)) {
			props.put("linger.ms", kafkaConfig.getInt(LINGER_MS));
		}
		if (kafkaConfig.hasPath(BUFFER_MEMORY)) {
			props.put("buffer.memory", kafkaConfig.getLong(BUFFER_MEMORY));
		}
		if (kafkaConfig.hasPath(KEY_SERIALIZER)) {
			props.put("key.serializer", kafkaConfig.getLong(KEY_SERIALIZER));
		}
//		if (kafkaConfig.hasPath(VALUE_SERIALIZER)) {
//			props.put("value.serializer", kafkaConfig.getLong(VALUE_SERIALIZER));
//		}
		props.put("value.serializer", AggregateEntitySerializer.class.getCanonicalName());

		streamTopicMap = new TreeMap<>();
		if (kafkaConfig.hasPath("topics")) {
			Config topicConfig = kafkaConfig.getConfig("topics");
			Set<Map.Entry<String, ConfigValue>> topics = topicConfig.entrySet();
			for (Map.Entry<String, ConfigValue> t : topics) {
				streamTopicMap.put(t.getKey(), (String) t.getValue().unwrapped());
			}
		}

		producer = new KafkaProducer<>(props);
	}

	@Override
	public boolean save(String stream, AggregateEntity apiEntity) throws Exception {
		if (streamTopicMap.get(stream) != null) {
			ProducerRecord<String, AggregateEntity> record = new ProducerRecord<>(streamTopicMap.get(stream), apiEntity);
			Future<RecordMetadata> future = producer.send(record);
			// TODO : more for check the sending status
			return true;
		}
		return false;
	}

}
