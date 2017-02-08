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
package org.apache.eagle.app.config;


import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

public class ConfigBuilderTest {
    @Test
    public void testConfigMapper() {
        MockDataSinkConfig config = ConfigBuilder.typeOf(MockDataSinkConfig.class).mapFrom(ConfigFactory.load());
        Assert.assertNotNull(config);
        Assert.assertEquals("test_topic", config.getTopic());
        Assert.assertEquals("sandbox.hortonworks.com:6667", config.getBrokerList());
        Assert.assertEquals("kafka.serializer.StringEncoder", config.getSerializerClass());
        Assert.assertEquals("kafka.serializer.StringEncoder", config.getKeySerializerClass());
        Assert.assertNull(config.getNotExistField());
        Assert.assertEquals("org.apache.eagle.metadata.service.memory.MemoryMetadataStore", config.getMetadata().getStore());
    }
}
