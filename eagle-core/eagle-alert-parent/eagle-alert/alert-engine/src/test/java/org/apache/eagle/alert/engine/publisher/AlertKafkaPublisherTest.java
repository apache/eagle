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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.dedup.DedupCache;
import org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class AlertKafkaPublisherTest {

    private static final String TEST_TOPIC_NAME = "test";

    private static KafkaEmbedded kafka;
    private static Config config;

    private static List<String> outputMessages = new ArrayList<String>();

    @BeforeClass
    public static void setup() {
        kafka = new KafkaEmbedded(9092, 2181);

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
        StreamDefinition stream = createStream();
        PolicyDefinition policy = createPolicy(stream.getStreamId(), "testPolicy");

        AlertKafkaPublisher publisher = new AlertKafkaPublisher();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(PublishConstants.BROKER_LIST, "localhost:9092");
        properties.put(PublishConstants.TOPIC, TEST_TOPIC_NAME);
        properties.put(PublishConstants.WRITE_MODE, "async");

        Publishment publishment = new Publishment();
        publishment.setName("testAsyncPublishment");
        publishment.setType("org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher");
        publishment.setPolicyIds(Arrays.asList(policy.getName()));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setProperties(properties);
        publishment.setSerializer("org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer");
        publishment.setProperties(properties);
        Map<String, String> conf = new HashMap<String, String>();
        publisher.init(config, publishment, conf);

        AlertStreamEvent event = createEvent(stream, policy,
            new Object[] {System.currentTimeMillis(), "host1", "testPolicy-host1-01", "open", 0, 0});

        outputMessages.clear();

        publisher.onAlert(event);
        Thread.sleep(3000);

        Assert.assertEquals(1, outputMessages.size());

        publisher.close();
    }

    @Test
    public void testSync() throws Exception {
        StreamDefinition stream = createStream();
        PolicyDefinition policy = createPolicy(stream.getStreamId(), "testPolicy");

        AlertKafkaPublisher publisher = new AlertKafkaPublisher();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(PublishConstants.BROKER_LIST, "localhost:9092");
        properties.put(PublishConstants.TOPIC, TEST_TOPIC_NAME);
        properties.put(PublishConstants.WRITE_MODE, "sync");

        Publishment publishment = new Publishment();
        publishment.setName("testAsyncPublishment");
        publishment.setType("org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher");
        publishment.setPolicyIds(Arrays.asList(policy.getName()));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setProperties(properties);
        publishment.setSerializer("org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer");
        publishment.setProperties(properties);
        Map<String, String> conf = new HashMap<String, String>();
        publisher.init(config, publishment, conf);

        AlertStreamEvent event = createEvent(stream, policy,
            new Object[] {System.currentTimeMillis(), "host1", "testPolicy-host1-01", "open", 0, 0});

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
                props.put("zookeeper.connect", "127.0.0.1:2181");
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
                        System.err.println(message);

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

    private AlertStreamEvent createEvent(StreamDefinition stream, PolicyDefinition policy, Object[] data) {
        AlertStreamEvent event = new AlertStreamEvent();
        event.setPolicyId(policy.getName());
        event.setSchema(stream);
        event.setStreamId(stream.getStreamId());
        event.setTimestamp(System.currentTimeMillis());
        event.setCreatedTime(System.currentTimeMillis());
        event.setData(data);
        return event;
    }

    private StreamDefinition createStream() {
        StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("host");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn alertKeyColumn = new StreamColumn();
        alertKeyColumn.setName("alertKey");
        alertKeyColumn.setType(StreamColumn.Type.STRING);

        StreamColumn stateColumn = new StreamColumn();
        stateColumn.setName("state");
        stateColumn.setType(StreamColumn.Type.STRING);

        // dedupCount, dedupFirstOccurrence

        StreamColumn dedupCountColumn = new StreamColumn();
        dedupCountColumn.setName("dedupCount");
        dedupCountColumn.setType(StreamColumn.Type.LONG);

        StreamColumn dedupFirstOccurrenceColumn = new StreamColumn();
        dedupFirstOccurrenceColumn.setName(DedupCache.DEDUP_FIRST_OCCURRENCE);
        dedupFirstOccurrenceColumn.setType(StreamColumn.Type.LONG);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, alertKeyColumn, stateColumn, dedupCountColumn,
            dedupFirstOccurrenceColumn));
        sd.setDataSource("testDatasource");
        sd.setStreamId("testStream");
        sd.setDescription("test stream");
        return sd;
    }

    private PolicyDefinition createPolicy(String streamName, String policyName) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        // expression, something like "PT5S,dynamic,1,host"
        def.setValue("test");
        def.setType("siddhi");
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList("inputStream"));
        pd.setOutputStreams(Arrays.asList("outputStream"));
        pd.setName(policyName);
        pd.setDescription(String.format("Test policy for stream %s", streamName));

        StreamPartition sp = new StreamPartition();
        sp.setStreamId(streamName);
        sp.setColumns(Arrays.asList("host"));
        sp.setType(StreamPartition.Type.GROUPBY);
        pd.addPartition(sp);
        return pd;
    }

}
