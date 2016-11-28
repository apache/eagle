/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.messaging;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaStreamSink extends StormStreamSink<KafkaStreamSinkConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamSink.class);
    private String topicId;
    private Producer producer;
    private KafkaStreamSinkConfig config;

    @Override
    public void init(String streamId, KafkaStreamSinkConfig config) {
        super.init(streamId, config);
        this.topicId = config.getTopicId();
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBrokerList());
        properties.put("serializer.class", config.getSerializerClass());
        properties.put("key.serializer.class", config.getKeySerializerClass());
        // new added properties for async producer
        properties.put("producer.type", config.getProducerType());
        properties.put("batch.num.messages", config.getNumBatchMessages());
        properties.put("request.required.acks", config.getRequestRequiredAcks());
        properties.put("queue.buffering.max.ms", config.getMaxQueueBufferMs());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new Producer(producerConfig);
    }

    @Override
    protected void execute(Object key, Map event, OutputCollector collector) throws Exception {
        try {
            String output = new ObjectMapper().writeValueAsString(event);
            // partition key may cause data skew
            //producer.send(new KeyedMessage(this.topicId, key, output));
            producer.send(new KeyedMessage(this.topicId, output));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    @Override
    public void afterInstall() {
        ensureTopicCreated();
    }

    private void ensureTopicCreated() {
        LOG.info("TODO: ensure kafka topic {} created", this.topicId);
    }

    private void ensureTopicDeleted() {
        LOG.info("TODO: ensure kafka topic {} deleted", this.topicId);
    }

    @Override
    public void cleanup() {
        if (this.producer != null) {
            this.producer.close();
        }
    }

    @Override
    public void afterUninstall() {
        ensureTopicDeleted();
    }
}