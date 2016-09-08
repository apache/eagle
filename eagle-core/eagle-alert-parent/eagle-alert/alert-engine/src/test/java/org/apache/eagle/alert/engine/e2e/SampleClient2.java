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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.utils.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import backtype.storm.utils.Utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @since May 10, 2016
 */
public class SampleClient2 {

//    private static final Logger LOG = LoggerFactory.getLogger(SampleClient2.class);

    public static class LogEntity {
        public String instanceUuid;
        public long timestamp;
        public String logLevel;
        public String message;
        public String reqId;
        public String host;
        public String component;
    }

    public static class IfEntity {
        public String instanceUuid;
        public long timestamp;
        public String reqId;
        public String message;
        public String host;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        AtomicLong base1 = new AtomicLong(System.currentTimeMillis());
        AtomicLong base2 = new AtomicLong(System.currentTimeMillis());
        AtomicLong count = new AtomicLong();

        Config config = ConfigFactory.load();

        try (KafkaProducer<String, String> proceduer = SampleClient1.createProceduer(config)) {
            while (true) {
                nextUuid = String.format(instanceUuidTemp, UUID.randomUUID().toString());
                nextReqId = String.format(reqIdTemp, UUID.randomUUID().toString());

                int hostIndex = 6;
                for (int i = 0; i < hostIndex; i++) {
                    sendMetric(base1, base2, count, proceduer, i);
                }

                if (count.get() % 600 == 0) {
                    System.out.println("send 600 LOG/FAILURE metric!");
                }

                Utils.sleep(3000);

            }
        }
    }

    private static void sendMetric(AtomicLong base1, AtomicLong base2, AtomicLong count,
                                   KafkaProducer<String, String> proceduer, int i) {
        {
            Pair<Long, String> pair = createLogEntity(base1, i);
            ProducerRecord<String, String> logRecord = new ProducerRecord<>("eslogs", pair.getRight());
            proceduer.send(logRecord);
            count.incrementAndGet();
        }
        {
            Pair<Long, String> pair2 = createFailureEntity(base2, i);
            ProducerRecord<String, String> failureRecord = new ProducerRecord<>("bootfailures", pair2.getRight());
            proceduer.send(failureRecord);
            count.incrementAndGet();
        }
    }

    private static String instanceUuidTemp = "instance-guid-%s";
    private static String reqIdTemp = "req-id-%s";
    private static String nextUuid;
    private static String nextReqId;

    private static Pair<Long, String> createLogEntity(AtomicLong base1, int hostIndex) {
        // TODO: add randomization
        LogEntity le = new LogEntity();
        if (hostIndex < 3) {
            le.component = "NOVA";
            le.host = "nova.000-" + hostIndex + ".datacenter.corp.com";
            le.message = "RabbitMQ Exception - MQ not connectable!";
        } else {
            le.component = "NEUTRON";
            le.host = "neturon.000-" + (hostIndex - 3) + ".datacenter.corp.com";
            le.message = "DNS Exception - Fail to connect to DNS!";
        }
        le.instanceUuid = nextUuid;
        le.logLevel = "ERROR";
        le.reqId = nextReqId;
        le.timestamp = base1.get();

        base1.addAndGet(1000);// simply some interval.
        return Pair.of(base1.get(), JsonUtils.writeValueAsString(le));
    }

    private static Pair<Long, String> createFailureEntity(AtomicLong base, int hostIndex) {
        // TODO: add randomization
        IfEntity ie = new IfEntity();
        ie.host = "boot-vm-0-" + hostIndex + ".datacenter.corp.com";
        ie.instanceUuid = nextUuid;
        ie.message = "boot failure for when try start the given vm!";
        ie.reqId = nextReqId;
        ie.timestamp = base.get();

        base.addAndGet(2000);// simply some interval.
        return Pair.of(base.get(), JsonUtils.writeValueAsString(ie));
    }

}
