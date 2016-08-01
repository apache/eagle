/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.topology;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang.time.DateUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;


public class KafkaProducerTest implements Serializable {

    String topic = "oozie";
    @Ignore
    @Test
    public void producerTestOozie() throws InterruptedException, JsonProcessingException, ParseException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        KafkaEmbedded kafka = new KafkaEmbedded(9092, 2181);

        makeSureTopic(topic);


        long starttime = DateTimeUtil.humanDateToMilliseconds("2016-07-07 17:40:14,526");
        Map<String, Object> map1 = new TreeMap<>();
        map1.put("ip", "yyy.yyy.yyy.yyy");
        map1.put("jobid", "0000000-160427140648764-oozie-oozi-W");
        map1.put("operation", "start");
        map1.put("timestamp", starttime);

        Map<String, Object> map2 = new TreeMap<>();
        map2.put("ip", "xxx.xxx.xxx.xxx");
        map2.put("jobid", "0000000-160427140648764-oozie-oozi-W");
        map2.put("operation", "end");
        map2.put("timestamp", starttime);

        ObjectMapper mapper = new ObjectMapper();

        while (true) {
            try {
                String msg = mapper.writeValueAsString(map1);
                String msg2 = mapper.writeValueAsString(map2);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "oozie", msg);
                ProducerRecord<String, String> producerRecord2 = new ProducerRecord<String, String>(topic, "oozie", msg2);
                System.out.println(msg);
                System.out.println(msg2);
                producer.send(producerRecord);
                producer.send(producerRecord2);
                Thread.sleep(3000);
                map1.clear();
                map2.clear();

                map1.put("ip", "yyy.yyy.yyy.yyy");
                map1.put("jobid", "140648764-oozie-oozi-W");
                map1.put("operation", "start");
                starttime = DateUtils.addSeconds(new Date(starttime),1).getTime();
                map1.put("timestamp", starttime);

                map2.put("ip", "xxx.xxx.xxx.xxx");
                map2.put("jobid", "150648764-oozie-oozi-W");
                map2.put("operation", "end");

                map2.put("timestamp", starttime);

            } catch (Exception e) {
                break;
            }
        }

        kafka.shutdown();
        producer.close();

    }

    public static void makeSureTopic(String topic) {
        ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
        Properties topicConfiguration = new Properties();
        ZkConnection zkConnection = new ZkConnection("localhost:2181");
      //  zkConnection.create()
//        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        AdminUtils.createTopic(zkClient, topic, 3, 1, topicConfiguration);
    }
    @Ignore
    @Test
    public void listZk() throws Exception {
        String zkhost="localhost:2181";//zk的host
        RetryPolicy rp=new ExponentialBackoffRetry(1000, 3);//重试机制
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString(zkhost)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(rp);
        builder.namespace("tjj");
        CuratorFramework zclient = builder.build();
        zclient.start();
        zclient.newNamespaceAwareEnsurePath("/tjj");
        System.out.print(zclient.checkExists().forPath("/alert"));

    }

}
