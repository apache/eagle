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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.UnitTopologyMain;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Case of simple
 * 
 * @since May 8, 2016
 *
 */
public class Integration1 {
    private static final Logger LOG = LoggerFactory.getLogger(Integration1.class);
    private static final ObjectMapper om = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Integration1 inte = new Integration1();
        inte.args = args;
        inte.test_simple_threshhold();
    }
    
    private String[] args;
    private ExecutorService executors = Executors.newFixedThreadPool(5);

    /**
     * Assumption:
     * <p>
     * start metadata service 8080, better in docker
     * <p>
     * start coordinator service 9090, better in docker
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
    @Ignore
    @Test
    public void test_simple_threshhold() throws Exception {
        System.setProperty("config.resource", "/application-integration.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load();

        System.out.println("loading metadatas...");
        loadMetadatas("/", config);
        System.out.println("loading metadatas done!");

        executors.submit(() -> SampleClient1.main(args));

        executors.submit(() -> UnitTopologyMain.main(args));

        Utils.sleep(1000 * 5l);
        while (true) {
            proactive_schedule(config);

            Utils.sleep(1000 * 60l * 5);
        }
    }

    /**
     * Test only run expected when there is a missed config in the config file. mark as ignored
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Ignore
    @Test(expected = ExecutionException.class)
    public void test_typesafe_config() throws InterruptedException, ExecutionException {
        System.setProperty("config.resource", "/application-integration.conf");
        ConfigFactory.invalidateCaches();
        Future<?> f = executors.submit(() -> {
            UnitTopologyMain.main(null);
        });

        f.get();
    }

//    @Test
//    private void makeSureTopic() {
//        System.setProperty("config.resource", "/application-integration.conf");
//        ConfigFactory.invalidateCaches();
//        Config config = ConfigFactory.load();
//        ZKConfig zkconfig = ZKConfigBuilder.getZKConfig(config);
//        
//        CuratorFramework curator = CuratorFrameworkFactory.newClient(
//                zkconfig.zkQuorum,
//                zkconfig.zkSessionTimeoutMs,
//                zkconfig.connectionTimeoutMs,
//                new RetryNTimes(zkconfig.zkRetryTimes, zkconfig.zkRetryInterval)
//        );
//    }

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
            List<Topology> l = om.readValue(Integration1.class.getResourceAsStream("/topologies.json"),
                    type);
            Topology t = (Topology) l.get(0);

            Assert.assertEquals(4, t.getGroupNodeIds().size());
            Assert.assertEquals(10, t.getAlertBoltIds().size());
        }

        {
            JavaType type = CollectionType.construct(List.class, SimpleType.construct(Publishment.class));
            // publishment
            List<Publishment> l = om.readValue(Integration1.class.getResourceAsStream("/publishments.json"), type);
            Publishment p = l.get(0);
            Assert.assertEquals("KAFKA", p.getType());
        }
        
        checkAll("/");
        checkAll("/correlation/");
    }

    private void checkAll(String base) throws Exception {
        loadEntities(base + "datasources.json", Kafka2TupleMetadata.class);
        loadEntities(base + "policies.json", PolicyDefinition.class);
        loadEntities(base + "publishments.json", Publishment.class);
        loadEntities(base + "streamdefinitions.json", StreamDefinition.class);
        loadEntities(base + "topologies.json", Topology.class);
    }

}
