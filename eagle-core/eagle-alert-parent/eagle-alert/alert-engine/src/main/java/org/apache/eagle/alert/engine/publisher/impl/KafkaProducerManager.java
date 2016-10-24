/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.eagle.alert.engine.publisher.impl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * The producer is thread safe and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 */
public class KafkaProducerManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerManager.class);

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String VALUE_DESERIALIZER_UNDERSCORE = "value_deserializer";

    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String VALUE_SERIALIZER_UNDERSCORE = "value_serializer";

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String KEY_DESERIALIZER_UNDERSCORE = "key_deserializer";

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String KEY_SERIALIZER_UNDERSCORE = "key_serializer";

    private static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
    private static final String REQUEST_REQUIRED_ACKS_UNDERSCORE = "request_required_acks";
    // the producer gets an acknowledgement after the leader replica has received the data
    private static final String REQUEST_REQUIRED_ACKS_DEFAULT = "1";

    public static final KafkaProducerManager INSTANCE = new KafkaProducerManager();

    public KafkaProducer<String, Object> getProducer(String brokerList, Map<String, String> kafkaConfig) {
        Properties configMap = new Properties();
        configMap.put("bootstrap.servers", brokerList);
        configMap.put("metadata.broker.list", brokerList);

        // key serializer
        if (kafkaConfig.containsKey(KEY_SERIALIZER_UNDERSCORE)) {
            configMap.put(KEY_SERIALIZER, kafkaConfig.get(KEY_SERIALIZER_UNDERSCORE));
        } else {
            configMap.put(KEY_SERIALIZER, STRING_SERIALIZER);
        }

        if (kafkaConfig.containsKey(KEY_DESERIALIZER_UNDERSCORE)) {
            configMap.put(KEY_DESERIALIZER, kafkaConfig.get(KEY_DESERIALIZER_UNDERSCORE));
        } else {
            configMap.put(KEY_DESERIALIZER, STRING_DESERIALIZER);
        }

        // value serializer
        if (kafkaConfig.containsKey(VALUE_SERIALIZER_UNDERSCORE)) {
            configMap.put(VALUE_SERIALIZER, kafkaConfig.get(VALUE_SERIALIZER_UNDERSCORE));
        } else {
            configMap.put(VALUE_SERIALIZER, STRING_SERIALIZER);
        }
        String requestRequiredAcks = REQUEST_REQUIRED_ACKS_DEFAULT;
        if (kafkaConfig.containsKey(REQUEST_REQUIRED_ACKS_UNDERSCORE)) {
            requestRequiredAcks = kafkaConfig.get(REQUEST_REQUIRED_ACKS_UNDERSCORE);
        }
        configMap.put(REQUEST_REQUIRED_ACKS, requestRequiredAcks);

        // value deserializer
        if (kafkaConfig.containsKey(VALUE_DESERIALIZER_UNDERSCORE)) {
            configMap.put(VALUE_DESERIALIZER, kafkaConfig.get(VALUE_DESERIALIZER_UNDERSCORE));
        } else {
            configMap.put(VALUE_DESERIALIZER, STRING_DESERIALIZER);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(" given kafka config {}, create producer config map {}", kafkaConfig, configMap);
        }

        KafkaProducer<String, Object> producer = new KafkaProducer<>(configMap);
        return producer;
    }
}