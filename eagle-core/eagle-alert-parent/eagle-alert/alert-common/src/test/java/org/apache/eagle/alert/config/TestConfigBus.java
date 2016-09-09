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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestConfigBus {

    @Ignore
    @Test
    public void testConfigChange() throws Exception {
        String topic = "spout";
        ZKConfig config = new ZKConfig();
        config.zkQuorum = "localhost:2181";
        config.zkRoot = "/alert";
        config.zkRetryInterval = 1000;
        config.zkRetryTimes = 3;
        config.connectionTimeoutMs = 3000;
        config.zkSessionTimeoutMs = 10000;
        ConfigBusProducer producer = new ConfigBusProducer(config);
        final AtomicBoolean validate = new AtomicBoolean(false);
        ConfigBusConsumer consumer = new ConfigBusConsumer(config, topic, value -> {
            validate.set(true);
            System.out.println("******** get notified of config " + value);
        });
        // first change
        ConfigValue value = new ConfigValue();
        value.setValueVersionId(false);
        value.setValue("testvalue1");
        producer.send(topic, value);

        Thread.sleep(1000);

        // second change
        value.setValueVersionId(false);
        value.setValue("testvalue2");
        producer.send(topic, value);
        producer.close();
        Thread.sleep(1000);
        consumer.close();
        Assert.assertTrue(validate.get());
    }
}
