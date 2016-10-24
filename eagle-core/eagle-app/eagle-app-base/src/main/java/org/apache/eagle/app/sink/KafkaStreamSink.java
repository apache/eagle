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
package org.apache.eagle.app.sink;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.eagle.metadata.utils.StreamIdConversions;
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
            producer.send(new KeyedMessage(this.topicId, key, output));
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

    public static class Provider implements StreamSinkProvider<KafkaStreamSink, KafkaStreamSinkConfig> {
        private static final Logger LOG = LoggerFactory.getLogger(Provider.class);
        private static final String DEAULT_SHARED_TOPIC_CONF_KEY = "dataSinkConfig.topic";

        private String getStreamSpecificTopicConfigKey(String streamId) {
            return String.format("dataSinkConfig.%s.topic",streamId);
        }

        @Override
        public KafkaStreamSinkConfig getSinkConfig(String streamId, Config config) {
            KafkaStreamSinkConfig desc = new KafkaStreamSinkConfig();
            String streamSpecificTopicConfigKey = getStreamSpecificTopicConfigKey(streamId);
            if (config.hasPath(streamSpecificTopicConfigKey)) {
                desc.setTopicId(config.getString(streamSpecificTopicConfigKey));
            } else if (config.hasPath(DEAULT_SHARED_TOPIC_CONF_KEY)) {
                desc.setTopicId(config.getString(DEAULT_SHARED_TOPIC_CONF_KEY));
                LOG.warn("Using default shared topic {}: {}", DEAULT_SHARED_TOPIC_CONF_KEY, desc.getTopicId());
            } else {
                LOG.error("Neither stream specific topic: {} nor default shared topic: {} found in config", streamSpecificTopicConfigKey, DEAULT_SHARED_TOPIC_CONF_KEY);
                throw new IllegalArgumentException("Neither stream specific topic: "
                    + streamSpecificTopicConfigKey + " nor default shared topic: " + DEAULT_SHARED_TOPIC_CONF_KEY + " found in config");
            }
            desc.setBrokerList(config.getString("dataSinkConfig.brokerList"));
            desc.setSerializerClass(config.hasPath("dataSinkConfig.serializerClass")
                ? config.getString("dataSinkConfig.serializerClass") : "kafka.serializer.StringEncoder");
            desc.setKeySerializerClass(config.hasPath("dataSinkConfig.keySerializerClass")
                ? config.getString("dataSinkConfig.keySerializerClass") : "kafka.serializer.StringEncoder");

            // new added properties for async producer
            desc.setNumBatchMessages(config.hasPath("dataSinkConfig.numBatchMessages")
                ? config.getString("dataSinkConfig.numBatchMessages") : "1024");
            desc.setProducerType(config.hasPath("dataSinkConfig.producerType")
                ? config.getString("dataSinkConfig.producerType") : "async");
            desc.setMaxQueueBufferMs(config.hasPath("dataSinkConfig.maxQueueBufferMs")
                ? config.getString("dataSinkConfig.maxQueueBufferMs") : "3000");
            desc.setRequestRequiredAcks(config.hasPath("dataSinkConfig.requestRequiredAcks")
                ? config.getString("dataSinkConfig.requestRequiredAcks") : "1");
            return desc;
        }

        @Override
        public KafkaStreamSink getSink() {
            return new KafkaStreamSink();
        }
    }
}