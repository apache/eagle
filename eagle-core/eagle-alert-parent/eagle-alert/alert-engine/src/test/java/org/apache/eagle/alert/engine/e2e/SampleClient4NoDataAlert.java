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

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Since 6/29/16.
 */
@SuppressWarnings({ "rawtypes", "unchecked"})
public class SampleClient4NoDataAlert {
    private static final Logger LOG = LoggerFactory.getLogger(SampleClient4NoDataAlert.class);
    private static long currentTimestamp = 1467240000000L;
    private static long interval = 3000L;
    private static volatile boolean host1Muted = false;
    private static volatile boolean host2Muted = false;
    public static void main(String[] args) throws Exception {
        System.setProperty("config.resource", "/nodata/application-nodata.conf");
        ConfigFactory.invalidateCaches();

        Config config = ConfigFactory.load();
        KafkaProducer producer = createProducer(config);
        ProducerRecord record = null;
        Thread x = new MuteThread();
        x.start();
        while(true) {
            if(!host1Muted) {
                record = new ProducerRecord("noDataAlertTopic", createEvent("host1"));
                producer.send(record);
            }
            if(!host2Muted) {
                record = new ProducerRecord("noDataAlertTopic", createEvent("host2"));
                producer.send(record);
            }
            record = new ProducerRecord("noDataAlertTopic", createEvent("host3"));
            producer.send(record);
            Utils.sleep(interval);
            currentTimestamp += interval;
        }
    }

    private static class MuteThread extends Thread{
        @Override
        public void run(){
            try {
                // sleep 10 seconds
                Thread.sleep(10000);
                // mute host1
                LOG.info("mute host1");
                host1Muted = true;
                // sleep 70 seconds for triggering no data alert
                LOG.info("try to sleep 70 seconds for triggering no data alert");
                Thread.sleep(70000);
                // unmute host1
                LOG.info("unmute host1");
                host1Muted = false;
                Thread.sleep(10000);
                // mute host2
                LOG.info("mute host2");
                host2Muted = true;
                // sleep 70 seconds for triggering no data alert
                LOG.info("try to sleep 70 seconds for triggering no data alert");
                Thread.sleep(70000);
                LOG.info("unmute host2");
                host2Muted = false;
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
    }

    private static class NoDataEvent{
        @JsonProperty
        long timestamp;
        @JsonProperty
        String host;
        @JsonProperty
        double value;

        public String toString(){
            return "timestamp=" + timestamp + ",host=" + host + ",value=" + value;
        }
    }

    private static String createEvent(String host) throws Exception{
        NoDataEvent e = new NoDataEvent();
        long expectTS = currentTimestamp + interval;
        // adjust back 1 second random
        long adjust = Math.round(2*Math.random());
        e.timestamp = expectTS-adjust;
        e.host = host;
        e.value = 25.6;
        LOG.info("sending event {} ",  e);
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
