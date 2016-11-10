/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.eagle.alert.config;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConfigBus {
    protected TestingServer server;
    protected ConfigBusProducer producer;
    protected ConfigBusConsumer consumer;
    protected String topic = "spout";
    protected ZKConfig config;
    final AtomicBoolean validate = new AtomicBoolean(false);
    final AtomicReference<String> configValue = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        config = new ZKConfig();
        config.zkQuorum = server.getConnectString();
        config.zkRoot = "/alert";
        config.zkRetryInterval = 1000;
        config.zkRetryTimes = 3;
        config.connectionTimeoutMs = 3000;
        config.zkSessionTimeoutMs = 10000;
        producer = new ConfigBusProducer(config);
        consumer = new ConfigBusConsumer(config, topic, value -> {
            validate.set(value.isValueVersionId());
            configValue.set((String) value.getValue());
            System.out.println("******** get notified of config " + value);
        });
    }

    @Test
    public void testConfigChange() throws Exception {
        // first change
        producer.send(topic, createConfigValue(false, "testvalue1"));

        Thread.sleep(1000);
        Assert.assertFalse(validate.get());
        Assert.assertEquals("testvalue1", configValue.get());

        // second change
        producer.send(topic, createConfigValue(true, "testvalue2"));
        Thread.sleep(1000);
        Assert.assertEquals("testvalue2", configValue.get());
    }

    @After
    public void shutdown() throws IOException {
        server.stop();
        producer.close();
        consumer.close();
    }

    private ConfigValue createConfigValue(boolean isValueVersionId, String value) {
        ConfigValue configValue = new ConfigValue();
        configValue.setValueVersionId(isValueVersionId);
        configValue.setValue(value);
        return configValue;
    }
}
