/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.publisher;

import java.util.*;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.dedup.DedupCache;
import org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher;
import org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class AlertKafkaPublisherTest {

    private static final String TEST_TOPIC_NAME = "test";
    private static final String TEST_POLICY_ID = "testPolicy";
    private static final int TEST_KAFKA_BROKER_PORT = 59092;
    private static final int TEST_KAFKA_ZOOKEEPER_PORT = 52181;
    private static KafkaEmbedded kafka;
    private static Config config;

    private static List<String> outputMessages = new ArrayList<String>();

    @BeforeClass
    public static void setup() {
        kafka = new KafkaEmbedded(TEST_KAFKA_BROKER_PORT, TEST_KAFKA_ZOOKEEPER_PORT);
        System.setProperty("config.resource", "/simple/application-integration.conf");
        config = ConfigFactory.load();
        consumeWithOutput(outputMessages);
    }

    @AfterClass
    public static void end() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    @Test
    public void testAsync() throws Exception {
        AlertKafkaPublisher publisher = new AlertKafkaPublisher();
        Map<String, Object> properties = new HashMap<>();
        properties.put(PublishConstants.BROKER_LIST, "localhost:" + TEST_KAFKA_BROKER_PORT);
        properties.put(PublishConstants.TOPIC, TEST_TOPIC_NAME);
        
        List<Map<String, Object>> kafkaClientConfig = new ArrayList<Map<String, Object>>();
        kafkaClientConfig.add(ImmutableMap.of("name", "producer.type", "value", "async"));
        kafkaClientConfig.add(ImmutableMap.of("name", "batch.num.messages", "value", 3000));
        kafkaClientConfig.add(ImmutableMap.of("name", "queue.buffering.max.ms", "value", 5000));
        kafkaClientConfig.add(ImmutableMap.of("name", "queue.buffering.max.messages", "value", 10000));
        properties.put("kafka_client_config", kafkaClientConfig);

        Publishment publishment = new Publishment();
        publishment.setName("testAsyncPublishment");
        publishment.setType(org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher.class.getName());
        publishment.setPolicyIds(Arrays.asList(TEST_POLICY_ID));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setSerializer(org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer.class.getName());
        publishment.setProperties(properties);
        
        Map<String, String> conf = new HashMap<String, String>();
        publisher.init(config, publishment, conf);

        AlertStreamEvent event = AlertPublisherTestHelper.mockEvent(TEST_POLICY_ID);

        outputMessages.clear();

        publisher.onAlert(event);
        Thread.sleep(3000);
        Assert.assertEquals(1, outputMessages.size());
        publisher.close();
    }

    @Test
    public void testSync() throws Exception {
        AlertKafkaPublisher publisher = new AlertKafkaPublisher();
        Map<String, Object> properties = new HashMap<>();
        properties.put(PublishConstants.BROKER_LIST, "localhost:" + TEST_KAFKA_BROKER_PORT);
        properties.put(PublishConstants.TOPIC, TEST_TOPIC_NAME);
        List<Map<String, Object>> kafkaClientConfig = new ArrayList<Map<String, Object>>();
        kafkaClientConfig.add(ImmutableMap.of("name", "producer.type", "value", "sync"));
        properties.put("kafka_client_config", kafkaClientConfig);
        Publishment publishment = new Publishment();
        publishment.setName("testAsyncPublishment");
        publishment.setType(org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher.class.getName());
        publishment.setPolicyIds(Collections.singletonList(TEST_POLICY_ID));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setSerializer(JsonEventSerializer.class.getName());
        publishment.setProperties(properties);
        Map<String, String> conf = new HashMap<>();
        publisher.init(config, publishment, conf);
        AlertStreamEvent event = AlertPublisherTestHelper.mockEvent(TEST_POLICY_ID);
        outputMessages.clear();
        publisher.onAlert(event);
        Thread.sleep(3000);
        Assert.assertEquals(1, outputMessages.size());
        publisher.close();
    }

    private static void consumeWithOutput(final List<String> outputMessages) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put("group.id", "B");
                props.put("zookeeper.connect", "127.0.0.1:" + + TEST_KAFKA_ZOOKEEPER_PORT);
                props.put("zookeeper.session.timeout.ms", "4000");
                props.put("zookeeper.sync.time.ms", "2000");
                props.put("auto.commit.interval.ms", "1000");
                props.put("auto.offset.reset", "smallest");

                ConsumerConnector jcc = null;
                try {
                    ConsumerConfig ccfg = new ConsumerConfig(props);
                    jcc = Consumer.createJavaConsumerConnector(ccfg);
                    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
                    topicCountMap.put(TEST_TOPIC_NAME, 1);
                    Map<String, List<KafkaStream<byte[], byte[]>>> topicMap = jcc.createMessageStreams(topicCountMap);
                    KafkaStream<byte[], byte[]> cstrm = topicMap.get(TEST_TOPIC_NAME).get(0);
                    for (MessageAndMetadata<byte[], byte[]> mm : cstrm) {
                        String message = new String(mm.message());
                        outputMessages.add(message);

                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                        }
                    }
                } finally {
                    if (jcc != null) {
                        jcc.shutdown();
                    }
                }
            }
        });
        t.start();
    }


}
