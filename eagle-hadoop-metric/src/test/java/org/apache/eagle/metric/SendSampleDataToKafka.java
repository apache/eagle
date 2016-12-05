/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metric;

import com.typesafe.config.ConfigFactory;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.eagle.app.messaging.KafkaStreamProvider;
import org.apache.eagle.app.messaging.KafkaStreamSinkConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

public class SendSampleDataToKafka {
    public static void main(String[] args) throws URISyntaxException, IOException {
        String data = "{" +
            "\"host\":\"localhost\", " +
            "\"timestamp\": 1480319109000, " +
            "\"metric\": \"hadoop.cpu.usage\", " +
            "\"component\": \"namenode\", " +
            "\"site\": \"test\", " +
            "\"value\": 0.98}";
        KafkaStreamSinkConfig config = new KafkaStreamProvider().getSinkConfig("HADOOP_JMX_METRIC_STREAM",ConfigFactory.load());
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBrokerList());
        properties.put("serializer.class", config.getSerializerClass());
        properties.put("key.serializer.class", config.getKeySerializerClass());
        // new added properties for async producer
        properties.put("producer.type", config.getProducerType());
        properties.put("batch.num.messages", config.getNumBatchMessages());
        properties.put("request.required.acks", config.getRequestRequiredAcks());
        properties.put("queue.buffering.max.ms", config.getMaxQueueBufferMs());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer producer = new kafka.javaapi.producer.Producer(producerConfig);
        try {
            producer.send(new KeyedMessage(config.getTopicId(), data));
        } finally {
            producer.close();
        }
    }
}
