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
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamProvider implements StreamProvider<KafkaStreamSink, KafkaStreamSinkConfig,KafkaStreamSource,KafkaStreamSourceConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamProvider.class);
    private static final String DEFAULT_SHARED_SINK_TOPIC_CONF_KEY = "dataSinkConfig.topic";
    private static final String DEFAULT_SHARED_SOURCE_TOPIC_CONF_KEY = "dataSourceConfig.topic";
    private static final String DEFAULT_SHARED_SOURCE_SCHEME_CLS_KEY = "dataSourceConfig.schemeCls";

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
        String streamSpecificTopicConfigKey = String.format("dataSourceConfig.%s.topic",streamId);
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

    private String getSourceSchemeCls(String streamId, Config config) {
        String streamSpecificSchemeClsKey = String.format("dataSourceConfig.%s.schemeCls", streamId);
        if (config.hasPath(streamSpecificSchemeClsKey) ) {
            return config.getString(streamSpecificSchemeClsKey);
        } else if (config.hasPath(DEFAULT_SHARED_SOURCE_SCHEME_CLS_KEY)) {
            LOG.warn("Using default shared source topic {}: {}", DEFAULT_SHARED_SOURCE_SCHEME_CLS_KEY, config.getString(DEFAULT_SHARED_SOURCE_SCHEME_CLS_KEY));
            return config.getString(DEFAULT_SHARED_SOURCE_SCHEME_CLS_KEY);
        }
        return null;
    }

    @Override
    public KafkaStreamSinkConfig getSinkConfig(String streamId, Config config) {
        KafkaStreamSinkConfig sinkConfig = new KafkaStreamSinkConfig();
        sinkConfig.setTopicId(getSinkTopicName(streamId,config));
        sinkConfig.setBrokerList(config.getString("dataSinkConfig.brokerList"));
        sinkConfig.setSerializerClass(hasNonBlankConfigPath(config, "dataSinkConfig.serializerClass")
            ? config.getString("dataSinkConfig.serializerClass") : "kafka.serializer.StringEncoder");
        sinkConfig.setKeySerializerClass(hasNonBlankConfigPath(config, "dataSinkConfig.keySerializerClass")
            ? config.getString("dataSinkConfig.keySerializerClass") : "kafka.serializer.StringEncoder");

        // new added properties for async producer
        sinkConfig.setNumBatchMessages(hasNonBlankConfigPath(config, "dataSinkConfig.numBatchMessages")
            ? config.getString("dataSinkConfig.numBatchMessages") : "1024");
        sinkConfig.setProducerType(hasNonBlankConfigPath(config, "dataSinkConfig.producerType")
            ? config.getString("dataSinkConfig.producerType") : "async");
        sinkConfig.setMaxQueueBufferMs(hasNonBlankConfigPath(config, "dataSinkConfig.maxQueueBufferMs")
            ? config.getString("dataSinkConfig.maxQueueBufferMs") : "3000");
        sinkConfig.setRequestRequiredAcks(hasNonBlankConfigPath(config, "dataSinkConfig.requestRequiredAcks")
            ? config.getString("dataSinkConfig.requestRequiredAcks") : "1");

        return sinkConfig;
    }

    @Override
    public KafkaStreamSink getSink() {
        return new KafkaStreamSink();
    }

    private boolean hasNonBlankConfigPath(Config config, String configName) {
        return config.hasPath(configName) && StringUtils.isNotBlank(config.getString(configName));
    }

    @Override
    public KafkaStreamSourceConfig getSourceConfig(String streamId, Config config) {
        KafkaStreamSourceConfig sourceConfig = new KafkaStreamSourceConfig();

        sourceConfig.setTopicId(getSourceTopicName(streamId,config));
        sourceConfig.setBrokerZkQuorum(config.getString("dataSourceConfig.zkConnection"));

        if (hasNonBlankConfigPath(config, "dataSourceConfig.fetchSize")) {
            sourceConfig.setFetchSize(config.getInt("dataSourceConfig.fetchSize"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.transactionZKRoot")) {
            sourceConfig.setTransactionZKRoot(config.getString("dataSourceConfig.transactionZKRoot"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.consumerGroupId")) {
            sourceConfig.setConsumerGroupId(config.getString("dataSourceConfig.consumerGroupId"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.brokerZkPath")) {
            sourceConfig.setBrokerZkPath(config.getString("dataSourceConfig.brokerZkPath"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.txZkServers")) {
            sourceConfig.setTransactionZkServers(config.getString("dataSourceConfig.txZkServers"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.transactionStateUpdateMS")) {
            sourceConfig.setTransactionStateUpdateMS(config.getLong("dataSourceConfig.transactionStateUpdateMS"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.startOffsetTime")) {
            sourceConfig.setStartOffsetTime(config.getInt("dataSourceConfig.startOffsetTime"));
        }
        if (hasNonBlankConfigPath(config, "dataSourceConfig.forceFromStart")) {
            sourceConfig.setForceFromStart(config.getBoolean("dataSourceConfig.forceFromStart"));
        }
        String schemeCls = getSourceSchemeCls(streamId, config);
        if (schemeCls != null && StringUtils.isNotBlank(schemeCls)) {
            try {
                sourceConfig.setSchemaClass((Class<? extends Scheme>) Class.forName(schemeCls));
            } catch (ClassNotFoundException e) {
                LOG.error("Class not found error, dataSourceConfig.schemeCls = {}", schemeCls, e);
            }
        }
        return sourceConfig;
    }

    @Override
    public KafkaStreamSource getSource() {
        return new KafkaStreamSource();
    }
}