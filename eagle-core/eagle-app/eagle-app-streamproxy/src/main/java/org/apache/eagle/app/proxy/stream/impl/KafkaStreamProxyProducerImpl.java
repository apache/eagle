/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.proxy.stream.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.eagle.alert.utils.StreamValidator;
import org.apache.eagle.app.messaging.KafkaStreamSinkConfig;
import org.apache.eagle.app.messaging.StreamRecord;
import org.apache.eagle.app.proxy.stream.StreamProxyProducer;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaStreamProxyProducerImpl implements StreamProxyProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamProxyProducerImpl.class);
    private final Producer producer;
    private final KafkaStreamSinkConfig config;
    private final String streamId;

    public KafkaStreamProxyProducerImpl(String streamId, StreamSinkConfig streamConfig) {
        Preconditions.checkNotNull(streamConfig, "Stream sink config for " + streamId + " is null");
        this.streamId = streamId;
        this.config = (KafkaStreamSinkConfig) streamConfig;
        Properties properties = new Properties();
        Preconditions.checkNotNull(config.getBrokerList(), "brokerList is null");
        properties.put("metadata.broker.list", config.getBrokerList());
        properties.put("serializer.class", config.getSerializerClass());
        properties.put("key.serializer.class", config.getKeySerializerClass());
        // new added properties for async producer
        properties.put("producer.type", config.getProducerType());
        properties.put("batch.num.messages", config.getNumBatchMessages());
        properties.put("request.required.acks", config.getRequestRequiredAcks());
        properties.put("queue.buffering.max.ms", config.getMaxQueueBufferMs());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);
    }

    @Override
    public void send(List<StreamRecord> events) throws IOException {
        for (StreamRecord record : events) {
            try {
                String output = new ObjectMapper().writeValueAsString(record);
                // partition key may cause data skew
                //producer.send(new KeyedMessage(this.topicId, key, output));
                producer.send(new KeyedMessage(this.config.getTopicId(), output));
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw ex;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.producer != null) {
            LOGGER.info("Closing kafka producer for stream {}", this.streamId);
            this.producer.close();
        }
    }
}
