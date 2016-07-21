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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @since Jun 12, 2016
 *
 */
public class SampleClient3 {
    
    

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void main(String[] args) throws Exception {
        System.setProperty("config.resource", "/e2e/application-e2e.conf");
        ConfigFactory.invalidateCaches();
        
        Config config = ConfigFactory.load();
        KafkaProducer producer = createByteProceduer(config);

//        TimeSeriesDataSchemaManager manager = TimeSeriesDataSchemaUtils
//                .createManagerFromCGMJSONStream(SherlockEventScheme.class.getResourceAsStream("/e2e/sherlock.json"));
//        Serializer serializer = ProtobufSerializer.newInstance(manager);
//
//        while (true) {
//            SherlockEvent event = createEvent(manager);
//            ProducerRecord record = new ProducerRecord("syslog_events", serializer.writeValueAsBytes(event));
//            producer.send(record);
//
//            Utils.sleep(3000);
//        }
    }

//    private static SherlockEvent createEvent(TimeSeriesDataSchemaManager manager) throws Exception {
//
//        SherlockEventBuilder builder = SherlockEvent.newBuilder();
//        builder.setEpochMillis(System.currentTimeMillis());
//        builder.setSchema(manager.getSchema("syslog", "parsed"));
//        // dim
//        DimTagsBuilder dimBuilder = DimTags.newBuilder();
//        dimBuilder.add("facility", "USER");
//        dimBuilder.add("severity", "NOTICE");
//        dimBuilder.add("hostname", "LM-SJC-11000548");
//        dimBuilder.add("msgid", "-");
//        builder.setDimTagsBuilder(dimBuilder);
//
//        MapdataValueBuilder mvBuilder = MapdataValue.newBuilder();
//        mvBuilder.put("timestamp", StringValue.newBuilder().setValue("04/Dec/2015:11:54:23 -0700").build());
//        mvBuilder.put("conn", StringValue.newBuilder().setValue("293578221").build());
//        mvBuilder.put("op", StringValue.newBuilder().setValue("1").build());
//        mvBuilder.put("msgId", StringValue.newBuilder().setValue("2").build());
//        mvBuilder.put(
//                "command",
//                StringValue.newBuilder()
//                        .setValue("RESULT err=0 tag=101 nentries=1 etime=0 additional alert line ha ha ha").build());
//
//        builder.setValueBuilder(mvBuilder);
//
//        return builder.build();
//    }
//    
    public static KafkaProducer<String, String> createByteProceduer(Config config) {
        String servers = config.getString("kafkaProducer.bootstrapServers");
        Properties configMap = new Properties();
        configMap.put("bootstrap.servers", servers);
        configMap.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        configMap.put("request.required.acks", "1");
        configMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaProducer<String, String> proceduer = new KafkaProducer<String, String>(configMap);
        return proceduer;
    }

}
