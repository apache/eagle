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
package org.apache.eagle.alert.engine.e2e;

import backtype.storm.utils.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Since 6/29/16.
 */
public class SampleClient5AbsenceAlert {
    private static final Logger LOG = LoggerFactory.getLogger(SampleClient5AbsenceAlert.class);
    private static long currentTimestamp = 1467240000000L;
    private static long interval = 3000L;

    public static void main(String[] args) throws Exception {
        System.setProperty("config.resource", "/absence/application-absence.conf");
        ConfigFactory.invalidateCaches();

        Config config = ConfigFactory.load();
        KafkaProducer producer = createProducer(config);
        ProducerRecord record = null;
        record = new ProducerRecord("absenceAlertTopic", createEvent("job1"));
        producer.send(record);
        record = new ProducerRecord("absenceAlertTopic", createEvent("job2"));
        producer.send(record);
        record = new ProducerRecord("absenceAlertTopic", createEvent("host3"));
        producer.send(record);
    }

    private static class AbsenceEvent {
        @JsonProperty
        long timestamp;
        @JsonProperty
        String jobID;
        @JsonProperty
        String status;

        public String toString() {
            return "timestamp=" + timestamp + ",jobID=" + jobID + ",status=" + status;
        }
    }

    private static String createEvent(String jobID) throws Exception {
        AbsenceEvent e = new AbsenceEvent();
        long expectTS = currentTimestamp + interval;
        // adjust back 1 second random
        long adjust = Math.round(2 * Math.random());
        e.timestamp = expectTS - adjust;
        e.jobID = jobID;
        e.status = "running";
        LOG.info("sending event {} ", e);
        ObjectMapper mapper = new ObjectMapper();
        String value = mapper.writeValueAsString(e);
        return value;
    }


    public static KafkaProducer<String, String> createProducer(Config config) {
        String servers = config.getString("kafkaProducer.bootstrapServers");
        Properties configMap = new Properties();
        configMap.put("bootstrap.servers", servers);
        configMap.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("request.required.acks", "1");
        configMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaProducer<String, String> proceduer = new KafkaProducer<String, String>(configMap);
        return proceduer;
    }
}