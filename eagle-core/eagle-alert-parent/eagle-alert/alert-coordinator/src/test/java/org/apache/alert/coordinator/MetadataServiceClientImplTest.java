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

import org.apache.eagle.alert.config.ConfigBusProducer;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.Coordinator;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.junit.Ignore;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @since May 9, 2016
 */
public class MetadataServiceClientImplTest {

    @Ignore
    @Test
    public void addScheduleState() throws Exception {
        ConfigFactory.invalidateCaches();
        System.setProperty("config.resource", "/test-application.conf");
        Config config = ConfigFactory.load("test-application.conf").getConfig("coordinator");
        MetadataServiceClientImpl client = new MetadataServiceClientImpl(config);

        ScheduleState ss = new ScheduleState();
        ss.setVersion("spec_version_1463764252582");

        client.addScheduleState(ss);

        client.close();

        ss.setVersion("spec_version_1464764252582");
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        ConfigBusProducer producer = new ConfigBusProducer(zkConfig);
        Coordinator.postSchedule(client, ss, producer);
    }
}
