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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.utils.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

/**
 * @since May 9, 2016
 *
 */
public class SampleClient1 {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SampleClient1.class);

    private static final String PERFMON_CPU_STREAM = "perfmon_cpu_stream";
    private static final String PERFMON_MEM_STREAM = "perfmon_mem_stream";

//    private static int hostIndx = 1;
    private static String hostTemp = "host-000%d.datacenter.corp.com";

    /**
     * <pre>
     * {"host": "", "timestamp" : "", "metric" : "", "pool": "", "value": 1.0, "colo": "phx"}
     * </pre>
     */
    public static class Entity {
        public String host;
        public long timestamp;
        public String metric;
        public String pool;
        public double value;
        public String colo;
    }

    public static void main(String[] args) {
        long base = System.currentTimeMillis();
        AtomicLong msgCount = new AtomicLong();

        try (KafkaProducer<String, String> proceduer = createProceduer()) {
            while (true) {
                int hostIndex = 6;
                for (int i = 0; i < hostIndex; i++) {
                    base = send_metric(base, proceduer, PERFMON_CPU_STREAM, i);
                    msgCount.incrementAndGet();
                    base = send_metric(base, proceduer, PERFMON_MEM_STREAM, i);
                    msgCount.incrementAndGet();
                }

                if ((msgCount.get() % 600) == 0) {
                    System.out.println("send 600 CPU/MEM metric!");
                }

                Utils.sleep(3000);
            }
        }
    }

    private static long send_metric(long base, KafkaProducer<String, String> proceduer, String stream, int hostIndex) {

        Pair<Long, String> pair = createEntity(base, stream, hostIndex);
        base = pair.getKey();
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("perfmon_metrics",
                pair.getRight());
        proceduer.send(record);
        return base;
    }

    private static Pair<Long, String> createEntity(long base, String stream, int hostIndex) {
        // TODO : add randomization
        Entity e = new Entity();
        e.colo = "LVS";
        e.host = String.format(hostTemp, hostIndex);
        if (hostIndex < 3) {
            e.pool = "hadoop-eagle-prod";
        } else {
            e.pool = "raptor-pool1";
        }
        e.timestamp = base;
        e.metric = stream;
        e.value = 92.0;

        base = base + 1000;

        return Pair.of(base, JsonUtils.writeValueAsString(e));
    }

    public static KafkaProducer<String, String> createProceduer() {

        Properties configMap = new Properties();
        // String broker_list = zkconfig.zkQuorum;
        // TODO: replace boot strap servers with new workable server
        configMap.put("bootstrap.servers", "localhost:9092");
        // configMap.put("metadata.broker.list", broker_list);
        configMap.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configMap.put("request.required.acks", "1");
        configMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaProducer<String, String> proceduer = new KafkaProducer<String, String>(configMap);
        return proceduer;
    }

}
