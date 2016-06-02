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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class AlertKafkaPublisher implements AlertPublishPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AlertKafkaPublisher.class);
    private AlertDeduplicator deduplicator;
    private PublishStatus status;
    @SuppressWarnings("rawtypes")
    private KafkaProducer producer;
    private String brokerList;
    private String topic;

    private final static long MAX_TIMEOUT_MS =60000;

    @Override
    public void init(Config config, Publishment publishment) throws Exception {
        deduplicator = new DefaultDeduplicator(publishment.getDedupIntervalMin());
        if (publishment.getProperties() != null) {
            Map<String, String> kafkaConfig = new HashMap<>(publishment.getProperties());
            brokerList = kafkaConfig.get(PublishConstants.BROKER_LIST).trim();
            producer = KafkaProducerManager.INSTANCE.getProducer(brokerList);
            topic = kafkaConfig.get(PublishConstants.TOPIC).trim();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        if (producer == null) {
            LOG.warn("KafkaProducer is null due to the incorrect configurations");
            return;
        }
        event = dedup(event);
        if(event == null) {
            return;
        }
        PublishStatus status = new PublishStatus();
        try {
            Future<?> future = producer.send(createRecord(event, topic));
            future.get(MAX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            status.successful = true;
            status.errorMessage = "";
            LOG.info("Successfully send message to Kafka: " + brokerList);
        } catch (InterruptedException | ExecutionException e) {
            status.successful = false;
            status.errorMessage = String.format("Failed to send message to %s, due to:%s", brokerList, e);
            LOG.error(status.errorMessage, e);
        } catch (Exception ex ) {
            LOG.error("fail writing alert to Kafka bus", ex);
            status.successful = false;
            status.errorMessage = ex.getMessage();
        }
        this.status = status;
    }

    @Override
    public void update(String dedupIntervalMin, Map<String, String> pluginProperties) {
        deduplicator.setDedupIntervalMin(dedupIntervalMin);
        String newBrokerList = pluginProperties.get(PublishConstants.BROKER_LIST).trim();
        String newTopic = pluginProperties.get(PublishConstants.TOPIC).trim();
        if (!newBrokerList.equals(this.brokerList)) {
            producer.close();
            brokerList = newBrokerList;
            KafkaProducer newProducer = null;
            try {
                newProducer = KafkaProducerManager.INSTANCE.getProducer(brokerList);
            } catch (Exception e) {
                LOG.error("Create KafkaProducer failed with configurations: {}", pluginProperties);
            }
            producer = newProducer;
        }
        topic = newTopic;
    }

    @Override
    public void close() {
        producer.close();
    }

    /**
     * To Create  KafkaProducer Record
     * @param event
     * @return ProducerRecord
     * @throws Exception
     */
    private ProducerRecord<String, String> createRecord(AlertStreamEvent event, String topic) throws Exception {
        ProducerRecord<String, String>  record  = new ProducerRecord<>(topic, event.toString());
        return record;
    }

    @Override
    public PublishStatus getStatus() {
        return this.status;
    }

    @Override
    public AlertStreamEvent dedup(AlertStreamEvent event) {
        return this.deduplicator.dedup(event);
    }
}
