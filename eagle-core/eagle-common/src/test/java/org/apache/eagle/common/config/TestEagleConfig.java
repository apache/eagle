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
package org.apache.eagle.common.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

/**
 * @since 9/22/15
 */
public class TestEagleConfig {

    @Test
    public void testLoadConfig() {
        System.setProperty("config.resource", "application-test.conf");
        EagleConfig config = EagleConfigFactory.load();
        Assert.assertEquals("test", config.getEnv());
        Assert.assertEquals("localhost-for-test", config.getZKQuorum());
        Assert.assertEquals("1234", config.getZKPort());
        Assert.assertEquals("hbase", config.getStorageType());
        Assert.assertEquals(true, config.isCoprocessorEnabled());
        Assert.assertEquals(9090, config.getServicePort());
        Assert.assertEquals("localhost", config.getServiceHost());
        Assert.assertEquals(TimeZone.getTimeZone("UTC"), config.getTimeZone());
    }
}