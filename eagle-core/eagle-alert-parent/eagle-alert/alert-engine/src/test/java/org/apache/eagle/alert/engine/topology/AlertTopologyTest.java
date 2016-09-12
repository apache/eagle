/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.spout.CorrelationSpout;
import org.apache.eagle.alert.engine.spout.CreateTopicUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

@SuppressWarnings( {"serial", "unused"})
public class AlertTopologyTest implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(AlertTopologyTest.class);
    char[] alphabets = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    @Ignore
    @Test
    public void testMultipleTopics() throws Exception {
        final String topoId = "myTopology";
        int numGroupbyBolts = 2;
        int numTotalGroupbyBolts = 3;
        System.setProperty("eagle.correlation.numGroupbyBolts", String.valueOf(numGroupbyBolts));
        System.setProperty("eagle.correlation.topologyName", topoId);
        System.setProperty("eagle.correlation.mode", "local");
        System.setProperty("eagle.correlation.zkHosts", "localhost:2181");
        final String topicName1 = "testTopic3";
        final String topicName2 = "testTopic4";
        // ensure topic ready
        LogManager.getLogger(CorrelationSpout.class).setLevel(Level.DEBUG);
        Config config = ConfigFactory.load();

        CreateTopicUtils.ensureTopicReady(System.getProperty("eagle.correlation.zkHosts"), topicName1);
        CreateTopicUtils.ensureTopicReady(System.getProperty("eagle.correlation.zkHosts"), topicName2);

        TopologyBuilder topoBuilder = new TopologyBuilder();

        int numBolts = config.getInt("eagle.correlation.numGroupbyBolts");
        CorrelationSpout spout = new CorrelationSpout(config, topoId, null, numBolts);
        String spoutId = "correlation-spout";
        SpoutDeclarer declarer = topoBuilder.setSpout(spoutId, spout);
        for (int i = 0; i < numBolts; i++) {
            TestBolt bolt = new TestBolt();
            BoltDeclarer boltDecl = topoBuilder.setBolt("engineBolt" + i, bolt);
            boltDecl.fieldsGrouping(spoutId, "stream_" + i, new Fields());
        }

        String topoName = config.getString("eagle.correlation.topologyName");
        LOG.info("start topology in local mode");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, new HashMap<>(), topoBuilder.createTopology());

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    @Ignore
    @Test
    public void generateRandomStringsToKafka() {
        String topic = "testTopic3";
        int max = 1000;
        Properties configMap = new Properties();
        configMap.put("bootstrap.servers", "localhost:6667");
        configMap.put("metadata.broker.list", "localhost:6667");
        configMap.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("request.required.acks", "1");
        configMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaProducer<String, Object> producer = new KafkaProducer<>(configMap);

        int i = 0;
        while (i++ < max) {
            String randomString = generateRandomString();
            System.out.println("sending string : " + randomString);
            ProducerRecord record = new ProducerRecord(topic, randomString);
            producer.send(record);
            if (i % 10 == 0) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {
                }
            }
        }
        producer.close();
    }

    private String generateRandomString() {
        long count = Math.round(Math.random() * 10);
        if (count == 0) {
            count = 1;
        }
        StringBuilder sb = new StringBuilder();
        while (count-- > 0) {
            int index = (int) (Math.floor(Math.random() * 26));
            sb.append(alphabets[index]);
        }
        return sb.toString();
    }
}
