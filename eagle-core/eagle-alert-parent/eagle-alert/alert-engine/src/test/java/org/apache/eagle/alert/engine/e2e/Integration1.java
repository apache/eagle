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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.UnitTopologyMain;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


/**
 * Case of simple
 *
 * @since May 8, 2016
 */
public class Integration1 {
    private static final String SIMPLE_CONFIG = "/simple/application-integration.conf";
    private static final Logger LOG = LoggerFactory.getLogger(Integration1.class);
    private static final ObjectMapper om = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Integration1 inte = new Integration1();
        inte.args = args;
        inte.test_simple_threshhold();
    }

    private String[] args;
    private ExecutorService executors = Executors.newFixedThreadPool(5, new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    });
    private static KafkaEmbedded kafka;

    @BeforeClass
    public static void setup() {
//        kafka = new KafkaEmbedded(9092, 2181);
//        makeSureTopic("perfmon_metrics");
    }

    @AfterClass
    public static void end() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    /**
     * Assumption:
     * <p>
     * start metadata service 8080 /rest
     * <p>
     * datasources : perfmon_datasource
     * <p>
     * stream: perfmon_cpu
     * <p>
     * policy : perfmon_cpu_host_check / perfmon_cpu_pool_check
     * <p>
     * Create topic
     * liasu@xxx:~$ $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic perfmon_metrics
     * <p>
     *
     * @throws InterruptedException
     */
    @Test
    public void test_simple_threshhold() throws Exception {
        System.setProperty("config.resource", SIMPLE_CONFIG);
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load();

        System.out.println("loading metadatas...");
        loadMetadatas("/simple/", config);
        System.out.println("loading metadatas done!");

        if (args == null) {
            args = new String[] {"-c", "simple/application-integration.conf"};
        }

        executors.submit(() -> SampleClient1.main(args));

        executors.submit(() -> {
            try {
                UnitTopologyMain.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Utils.sleep(1000 * 5l);
        while (true) {
            proactive_schedule(config);

            Utils.sleep(1000 * 60l * 5);
        }
    }

    public static void proactive_schedule(Config config) throws Exception {
        try (CoordinatorClient cc = new CoordinatorClient(config)) {
            try {
                String resp = cc.schedule();
                LOG.info("schedule return : {} ", resp);
            } catch (Exception e) {
                LOG.error("failed to call schedule!", e);
            }
        }
    }

    public static void loadMetadatas(String base, Config config) throws Exception {
        IMetadataServiceClient client = new MetadataServiceClientImpl(config);
        client.clear();

        List<Kafka2TupleMetadata> metadata = loadEntities(base + "datasources.json", Kafka2TupleMetadata.class);
        for (Kafka2TupleMetadata k : metadata) {
            client.addDataSource(k);
        }

        List<PolicyDefinition> policies = loadEntities(base + "policies.json", PolicyDefinition.class);
        for (PolicyDefinition p : policies) {
            client.addPolicy(p);
        }

        List<Publishment> pubs = loadEntities(base + "publishments.json", Publishment.class);
        for (Publishment pub : pubs) {
            client.addPublishment(pub);
        }

        List<StreamDefinition> defs = loadEntities(base + "streamdefinitions.json", StreamDefinition.class);
        for (StreamDefinition def : defs) {
            client.addStreamDefinition(def);
        }

        List<Topology> topos = loadEntities(base + "topologies.json", Topology.class);
        for (Topology t : topos) {
            client.addTopology(t);
        }

        client.close();
    }

    public static <T> List<T> loadEntities(String path, Class<T> tClz) throws Exception {
        JavaType type = CollectionType.construct(List.class, SimpleType.construct(tClz));
        List<T> l = om.readValue(Integration1.class.getResourceAsStream(path), type);
        return l;
    }

    /**
     * <p>
     * {"name":"xxx","numOfSpout":1,"numOfAlertBolt":3,"numOfGroupBolt":2,
     * "spoutId"
     * :"xxx-spout","groupNodeIds":["xxx-grp"],"alertBoltIds":["xxx-bolt"
     * ],"pubBoltId":"xxx-pubBolt","spoutParallelism":1,"groupParallelism":1,
     * "alertParallelism":1}
     * <p>
     *
     * @throws Exception
     */
    @Ignore
    @Test
    public void testJson() throws Exception {
        {
            JavaType type = CollectionType.construct(List.class, SimpleType.construct(Topology.class));
            List<Topology> l = om.readValue(Integration1.class.getResourceAsStream("/simple/topologies.json"),
                type);
            Topology t = (Topology) l.get(0);

            Assert.assertEquals(4, t.getGroupNodeIds().size());
            Assert.assertEquals(10, t.getAlertBoltIds().size());
        }

        {
            JavaType type = CollectionType.construct(List.class, SimpleType.construct(Publishment.class));
            // publishment
            List<Publishment> l = om.readValue(Integration1.class.getResourceAsStream("/simple/publishments.json"), type);
            Publishment p = l.get(0);
            Assert.assertEquals("KAFKA", p.getType());
        }

        checkAll("/simple/");
        checkAll("/correlation/");
    }

    public static void checkAll(String base) throws Exception {
        loadEntities(base + "datasources.json", Kafka2TupleMetadata.class);
        loadEntities(base + "policies.json", PolicyDefinition.class);
        loadEntities(base + "publishments.json", Publishment.class);
        loadEntities(base + "streamdefinitions.json", StreamDefinition.class);
        loadEntities(base + "topologies.json", Topology.class);
    }

}
