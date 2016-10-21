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

import org.apache.eagle.metadata.model.StreamSinkConfig;

public class KafkaStreamSinkConfig implements StreamSinkConfig {
    private String topicId;
    private String brokerList;
    private String serializerClass;
    private String keySerializerClass;
    private String numBatchMessages;
    private String maxQueueBufferMs;
    private String producerType;
    private String requestRequiredAcks;

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
}
