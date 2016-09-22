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
package org.apache.alert.coordinator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.eagle.alert.config.ConfigBusConsumer;
import org.apache.eagle.alert.config.ConfigBusProducer;
import org.apache.eagle.alert.config.ConfigChangeCallback;
import org.apache.eagle.alert.config.ConfigValue;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.Coordinator;
import org.apache.eagle.alert.coordinator.ScheduleOption;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @since May 5, 2016
 */
public class CoordinatorTest {

    private static ZookeeperEmbedded zkEmbed;

    @BeforeClass
    public static void setup() throws Exception {
        zkEmbed = new ZookeeperEmbedded(2181);
        int zkPort = zkEmbed.start();
        System.setProperty("coordinator.zkConfig.zkQuorum","localhost:"+ zkPort);
    }

    @AfterClass
    public static void teardown() {
        zkEmbed.shutdown();
    }

    @SuppressWarnings( {"resource", "unused"})
    @Ignore
    @Test
    public void test() throws Exception {
        before();
        Config config = ConfigFactory.load().getConfig("coordinator");
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        ConfigBusProducer producer = new ConfigBusProducer(zkConfig);
        IMetadataServiceClient client = new MetadataServiceClientImpl(config);

        Coordinator coordinator = new Coordinator(config, producer, client);
        ScheduleOption option = new ScheduleOption();
        ScheduleState state = coordinator.schedule(option);
        String v = state.getVersion();

        AtomicBoolean validated = new AtomicBoolean(false);
        ConfigBusConsumer consumer = new ConfigBusConsumer(zkConfig, "topo1/spout", new ConfigChangeCallback() {
            @Override
            public void onNewConfig(ConfigValue value) {
                String vId = value.getValue().toString();
                Assert.assertEquals(v, vId);
                validated.set(true);
            }
        });

        Thread.sleep(1000);
        Assert.assertTrue(validated.get());
    }

    @SuppressWarnings( {"resource", "unused"})
    @Test
    public void test_01() throws Exception {
        before();
        Config config = ConfigFactory.load().getConfig("coordinator");
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        ConfigBusProducer producer = new ConfigBusProducer(zkConfig);
        IMetadataServiceClient client = ScheduleContextBuilderTest.getSampleMetadataService();

        Coordinator coordinator = new Coordinator(config, producer, client);
        ScheduleOption option = new ScheduleOption();
        ScheduleState state = coordinator.schedule(option);
        String v = state.getVersion();

        // TODO : assert version

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean validated = new AtomicBoolean(false);
        ConfigBusConsumer consumer = new ConfigBusConsumer(zkConfig, "topo1/spout", new ConfigChangeCallback() {
            @Override
            public void onNewConfig(ConfigValue value) {
                String vId = value.getValue().toString();
                Assert.assertEquals(v, vId);
                validated.set(true);
                latch.countDown();
            }
        });

        latch.await(3, TimeUnit.SECONDS);
        Assert.assertTrue(validated.get());
    }

    @Ignore
    @Test
    public void test_main() throws Exception {
        before();
        Coordinator.main(null);
    }

    @Before
    public void before() {
        System.setProperty("config.resource", "/test-application.conf");
        ConfigFactory.invalidateCaches();
        ConfigFactory.load().getConfig("coordinator");
    }

    @Test
    public void test_Schedule() {
        Coordinator.startSchedule();
    }

}
