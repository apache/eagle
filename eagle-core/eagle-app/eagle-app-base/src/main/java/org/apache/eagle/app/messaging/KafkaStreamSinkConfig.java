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
import org.apache.eagle.metadata.model.StreamSinkConfig;

/**
 * FIXME Rename to KafkaStreamMessagingConfig.
 */
public class KafkaStreamSinkConfig implements StreamSinkConfig {

    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "eagleKafkaSource";
    private static final String DEFAULT_TRANSACTION_ZK_ROOT = "/consumers";

    // Write Config
    private String topicId;
    private String brokerList;
    private String serializerClass;
    private String keySerializerClass;
    private String numBatchMessages;
    private String maxQueueBufferMs;
    private String producerType;
    private String requestRequiredAcks;

    // Read Config
    private String brokerZkQuorum;
    private String brokerZkBasePath;
    private int fetchSize = 1048576;
    private String transactionZKRoot = DEFAULT_TRANSACTION_ZK_ROOT;
    private String consumerGroupId = DEFAULT_CONSUMER_GROUP_ID;
    private String brokerZkPath = null;
    private String transactionZkServers;
    private int transactionStateUpdateMS = 2000;
    private int startOffsetTime = -1;
    private boolean forceFromStart;
    private Class<backtype.storm.spout.Scheme> schemaClass;

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getSerializerClass() {
        return serializerClass;
    }

    public void setSerializerClass(String serializerClass) {
        this.serializerClass = serializerClass;
    }

    public String getKeySerializerClass() {
        return keySerializerClass;
    }

    public void setKeySerializerClass(String keySerializerClass) {
        this.keySerializerClass = keySerializerClass;
    }

    public String getNumBatchMessages() {
        return numBatchMessages;
    }

    public void setNumBatchMessages(String numBatchMessages) {
        this.numBatchMessages = numBatchMessages;
    }

    public String getMaxQueueBufferMs() {
        return maxQueueBufferMs;
    }

    public void setMaxQueueBufferMs(String maxQueueBufferMs) {
        this.maxQueueBufferMs = maxQueueBufferMs;
    }

    public String getProducerType() {
        return producerType;
    }

    public void setProducerType(String producerType) {
        this.producerType = producerType;
    }

    public String getRequestRequiredAcks() {
        return requestRequiredAcks;
    }

    public void setRequestRequiredAcks(String requestRequiredAcks) {
        this.requestRequiredAcks = requestRequiredAcks;
    }

    @Override
    public String getType() {
        return "KAFKA";
    }

    @Override
    public Class<?> getSinkType() {
        return KafkaStreamSink.class;
    }

    @Override
    public Class<? extends StreamSinkConfig> getConfigType() {
        return KafkaStreamSinkConfig.class;
    }

    public String getBrokerZkQuorum() {
        return brokerZkQuorum;
    }

    public void setBrokerZkQuorum(String brokerZkQuorum) {
        this.brokerZkQuorum = brokerZkQuorum;
    }

    public String getBrokerZkBasePath() {
        return brokerZkBasePath;
    }

    public void setBrokerZkBasePath(String brokerZkBasePath) {
        this.brokerZkBasePath = brokerZkBasePath;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getTransactionZKRoot() {
        return transactionZKRoot;
    }

    public void setTransactionZKRoot(String transactionZKRoot) {
        this.transactionZKRoot = transactionZKRoot;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public String getBrokerZkPath() {
        return brokerZkPath;
    }

    public void setBrokerZkPath(String brokerZkPath) {
        this.brokerZkPath = brokerZkPath;
    }

    public String getTransactionZkServers() {
        return transactionZkServers;
    }

    public void setTransactionZkServers(String transactionZkServers) {
        this.transactionZkServers = transactionZkServers;
    }

    public int getTransactionStateUpdateMS() {
        return transactionStateUpdateMS;
    }

    public void setTransactionStateUpdateMS(int transactionStateUpdateMS) {
        this.transactionStateUpdateMS = transactionStateUpdateMS;
    }

    public int getStartOffsetTime() {
        return startOffsetTime;
    }

    public void setStartOffsetTime(int startOffsetTime) {
        this.startOffsetTime = startOffsetTime;
    }

    public boolean isForceFromStart() {
        return forceFromStart;
    }

    public void setForceFromStart(boolean forceFromStart) {
        this.forceFromStart = forceFromStart;
    }

    public Class<Scheme> getSchemaClass() {
        return schemaClass;
    }

    public void setSchemaClass(Class<Scheme> schemaClass) {
        this.schemaClass = schemaClass;
    }
}
