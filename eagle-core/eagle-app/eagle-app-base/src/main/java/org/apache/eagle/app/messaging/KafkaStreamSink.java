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

import backtype.storm.spout.Scheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
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

    /**
     * TODO: Move out of KafkaStreamSink and name as KafkaStreamDriver.
     */
    public static class Provider implements StreamSinkProvider<KafkaStreamSink, KafkaStreamSinkConfig,KafkaStreamSource,KafkaStreamSourceConfig> {
        private static final Logger LOG = LoggerFactory.getLogger(Provider.class);
        private static final String DEFAULT_SHARED_SINK_TOPIC_CONF_KEY = "dataSinkConfig.topic";
        private static final String DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY = "dataSourceConfig.topic";

        private String getSinkTopicName(String streamId, Config config) {
            String streamSpecificTopicConfigKey = String.format("dataSinkConfig.%s.topic",streamId);
            if (config.hasPath(streamSpecificTopicConfigKey)) {
                return config.getString(streamSpecificTopicConfigKey);
            } else if (config.hasPath(DEFAULT_SHARED_SINK_TOPIC_CONF_KEY)) {
                LOG.warn("Using default shared sink topic {}: {}", DEFAULT_SHARED_SINK_TOPIC_CONF_KEY, config.getString(DEFAULT_SHARED_SINK_TOPIC_CONF_KEY));
                return config.getString(DEFAULT_SHARED_SINK_TOPIC_CONF_KEY);
            } else {
                LOG.error("Neither stream specific topic: {} nor default shared topic: {} found in config", streamSpecificTopicConfigKey, DEFAULT_SHARED_SINK_TOPIC_CONF_KEY);
                throw new IllegalArgumentException("Neither stream specific topic: "
                    + streamSpecificTopicConfigKey + " nor default shared topic: " + DEFAULT_SHARED_SINK_TOPIC_CONF_KEY + " found in config");
            }
        }

        private String getSourceTopicName(String streamId, Config config) {
            String streamSpecificTopicConfigKey = String.format("dataSourceConfig.%s.topic",streamId);;
            if (config.hasPath(streamSpecificTopicConfigKey)) {
                return config.getString(streamSpecificTopicConfigKey);
            } else if (config.hasPath(DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY)) {
                LOG.warn("Using default shared source topic {}: {}", DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY, config.getString(DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY));
                return config.getString(DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY);
            } else {
                LOG.debug("Neither stream specific topic: {} nor default shared topic: {} found in config, try sink config instead", streamSpecificTopicConfigKey, DEFAULT_SHARED_SINK_TOPIC_CONF_KEY);
                return getSinkTopicName(streamId,config);
            }
        }

        @Override
        public KafkaStreamSinkConfig getSinkConfig(String streamId, Config config) {
            KafkaStreamSinkConfig sinkConfig = new KafkaStreamSinkConfig();
            sinkConfig.setTopicId(getSinkTopicName(streamId,config));
            sinkConfig.setBrokerList(config.getString("dataSinkConfig.brokerList"));
            sinkConfig.setSerializerClass(config.hasPath("dataSinkConfig.serializerClass")
                ? config.getString("dataSinkConfig.serializerClass") : "kafka.serializer.StringEncoder");
            sinkConfig.setKeySerializerClass(config.hasPath("dataSinkConfig.keySerializerClass")
                ? config.getString("dataSinkConfig.keySerializerClass") : "kafka.serializer.StringEncoder");

            // new added properties for async producer
            sinkConfig.setNumBatchMessages(config.hasPath("dataSinkConfig.numBatchMessages")
                ? config.getString("dataSinkConfig.numBatchMessages") : "1024");
            sinkConfig.setProducerType(config.hasPath("dataSinkConfig.producerType")
                ? config.getString("dataSinkConfig.producerType") : "async");
            sinkConfig.setMaxQueueBufferMs(config.hasPath("dataSinkConfig.maxQueueBufferMs")
                ? config.getString("dataSinkConfig.maxQueueBufferMs") : "3000");
            sinkConfig.setRequestRequiredAcks(config.hasPath("dataSinkConfig.requestRequiredAcks")
                ? config.getString("dataSinkConfig.requestRequiredAcks") : "1");

            return sinkConfig;
        }

        @Override
        public KafkaStreamSink getSink() {
            return new KafkaStreamSink();
        }

        @Override
        public KafkaStreamSourceConfig getSourceConfig(String streamId, Config config) {
            KafkaStreamSourceConfig sourceConfig = new KafkaStreamSourceConfig();

            sourceConfig.setTopicId(getSourceTopicName(streamId,config));
            sourceConfig.setBrokerZkQuorum(config.getString("dataSourceConfig.zkConnection"));

            if (config.hasPath("dataSourceConfig.fetchSize")) {
                sourceConfig.setFetchSize(config.getInt("dataSourceConfig.fetchSize"));
            }
            if (config.hasPath("dataSourceConfig.transactionZKRoot")) {
                sourceConfig.setTransactionZKRoot(config.getString("dataSourceConfig.transactionZKRoot"));
            }
            if (config.hasPath("dataSourceConfig.consumerGroupId")) {
                sourceConfig.setConsumerGroupId(config.getString("dataSourceConfig.consumerGroupId"));
            }
            if (config.hasPath("dataSourceConfig.brokerZkPath")) {
                sourceConfig.setBrokerZkPath(config.getString("dataSourceConfig.brokerZkPath"));
            }
            if (config.hasPath("dataSourceConfig.txZkServers")) {
                sourceConfig.setTransactionZkServers(config.getString("dataSourceConfig.txZkServers"));
            }
            if (config.hasPath("dataSourceConfig.transactionStateUpdateMS")) {
                sourceConfig.setTransactionStateUpdateMS(config.getLong("dataSourceConfig.transactionStateUpdateMS"));
            }
            if (config.hasPath("dataSourceConfig.startOffsetTime")) {
                sourceConfig.setStartOffsetTime(config.getInt("dataSourceConfig.startOffsetTime"));
            }
            if (config.hasPath("dataSourceConfig.forceFromStart")) {
                sourceConfig.setForceFromStart(config.getBoolean("dataSourceConfig.forceFromStart"));
            }
            if (config.hasPath("dataSourceConfig.schemeCls")) {
                try {
                    sourceConfig.setSchemaClass((Class<? extends Scheme>) Class.forName(config.getString("dataSourceConfig.schemeCls")));
                } catch (ClassNotFoundException e) {
                    LOG.error("Class not found error, dataSourceConfig.schemeCls = {}",config.getString("dataSourceConfig.schemeCls"),e);
                }
            }
            return sourceConfig;
        }

        @Override
        public KafkaStreamSource getSource() {
            return new KafkaStreamSource();
        }
    }
}