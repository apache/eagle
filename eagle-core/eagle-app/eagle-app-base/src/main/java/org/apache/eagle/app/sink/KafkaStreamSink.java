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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
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
    private final static Logger LOG = LoggerFactory.getLogger(KafkaStreamSink.class);
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
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBrokerList());
        properties.put("serializer.class", config.getSerializerClass());
        properties.put("key.serializer.class", config.getKeySerializerClass());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new Producer(producerConfig);
    }

    @Override
    protected void execute(Object key, Map event,BasicOutputCollector collector) {
        try {
            String output = new ObjectMapper().writeValueAsString(event);
            producer.send(new KeyedMessage(this.topicId, event.get("user"), output));
        }catch(Exception ex){
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        }
    }

    @Override
    public void onInstall() {
        ensureTopicCreated();
    }

    private void ensureTopicCreated(){
        LOG.info("TODO: ensure kafka topic {} created",this.topicId);
    }

    private void ensureTopicDeleted(){
        LOG.info("TODO: ensure kafka topic {} deleted",this.topicId);
    }

    @Override
    public void cleanup() {
        if(this.producer != null){
            this.producer.close();
        }
    }

    @Override
    public void onUninstall() {
        ensureTopicDeleted();
    }

    public static class Provider implements StreamSinkProvider<KafkaStreamSink,KafkaStreamSinkConfig> {
        @Override
        public KafkaStreamSinkConfig getSinkConfig(String streamId, Config config) {
            KafkaStreamSinkConfig desc = new KafkaStreamSinkConfig();
            desc.setTopicId(config.getString("dataSinkConfig.topic"));
            desc.setBrokerList(config.getString("dataSinkConfig.brokerList"));
            desc.setSerializerClass(config.getString("dataSinkConfig.serializerClass"));
            desc.setKeySerializerClass(config.getString("dataSinkConfig.keySerializerClass"));
            return desc;
        }

        @Override
        public KafkaStreamSink getSink() {
            return new KafkaStreamSink();
        }
    }
}