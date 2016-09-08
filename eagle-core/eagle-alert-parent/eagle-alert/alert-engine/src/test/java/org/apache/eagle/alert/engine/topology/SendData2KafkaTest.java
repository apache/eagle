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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Created on 3/12/16.
 */
@SuppressWarnings( {"serial", "unchecked", "rawtypes", "resource"})
public class SendData2KafkaTest implements Serializable {
    /**
     * {"timestamp": 10000, "metric": "esErrorLogEvent", "instanceUuid": "vm-InstanceId1", "host":"test-host1", "type":"nova", "stack":"NullPointException-..........."}
     * {"timestamp": 10000, "metric": "instanceFailureLogEvent", "instanceUuid": "vm-InstanceId1", "message":"instance boot failure for user liasu!"}
     */
    @Test
    @Ignore
    public void testOutput() throws Exception {
        String s1 = "{\"timestamp\": %d, \"metric\": \"esErrorLogEvent\", \"instanceUuid\": \"vm-InstanceId%d\", \"host\":\"test-host1\", \"type\":\"nova\", \"stack\":\"NullPointException-...........\"}";
        String s2 = "{\"timestamp\": %d, \"metric\": \"instanceFailureLogEvent\", \"instanceUuid\": \"vm-InstanceId%d\", \"message\":\"instance boot failure for user liasu!\"}";

        PrintWriter espw = new PrintWriter(new FileWriter("src/test/resources/es.log"));
        PrintWriter ifpw = new PrintWriter(new FileWriter("src/test/resources/if.log"));

        long base = System.currentTimeMillis();
        long timestamp = 10000;

        for (int i = 0; i < 10; i++) {

            timestamp = base + i * 10000;
            for (int j = 0; j < 10; j++) {
                timestamp = timestamp + j * 1000;

                espw.println(String.format(s1, timestamp, i));
            }

            ifpw.println(String.format(s2, timestamp, i));
        }

        espw.flush();
        ifpw.flush();
    }

    @Test
    @Ignore
    public void sendKakfa() throws Exception {
        List<String> eslogs = Files.readAllLines(Paths.get(SendData2KafkaTest.class.getResource("/es.log").toURI()), Charset.defaultCharset());
        List<String> iflogs = Files.readAllLines(Paths.get(SendData2KafkaTest.class.getResource("/if.log").toURI()), Charset.defaultCharset());

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("value.serializer", StringSerializer.class.getCanonicalName());
        props.put("key.serializer", StringSerializer.class.getCanonicalName());

        KafkaProducer producer = new KafkaProducer<>(props);
        while (true) {
            for (String s : eslogs) {
                ProducerRecord<String, String> record = new ProducerRecord<>("nn_jmx_metric_sandbox", s);
                producer.send(record);
            }

            for (String s : iflogs) {
                ProducerRecord<String, String> record = new ProducerRecord<>("nn_jmx_metric_sandbox", s);
                producer.send(record);
            }

            Thread.sleep(5000);
        }

    }

}