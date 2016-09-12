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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordinator.provider.NodataMetadataGenerator;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class NodataMetadataGeneratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(NodataMetadataGeneratorTest.class);

    Config config = ConfigFactory.load().getConfig("coordinator");
    private NodataMetadataGenerator generator;

    @Before
    public void setup() {
        generator = new NodataMetadataGenerator();
    }

    @Test
    public void testNormal() throws Exception {
        StreamDefinition sd = createStreamDefinitionWithNodataAlert();
        Map<String, StreamDefinition> streamDefinitionsMap = new HashMap<String, StreamDefinition>();
        streamDefinitionsMap.put(sd.getStreamId(), sd);

        Map<String, Kafka2TupleMetadata> kafkaSources = new HashMap<String, Kafka2TupleMetadata>();
        Map<String, PolicyDefinition> policies = new HashMap<String, PolicyDefinition>();
        Map<String, Publishment> publishments = new HashMap<String, Publishment>();

        generator.execute(config, streamDefinitionsMap, kafkaSources, policies, publishments);

        Assert.assertEquals(2, kafkaSources.size());

        kafkaSources.forEach((key, value) -> {
            LOG.info("KafkaSources > {}: {}", key, ToStringBuilder.reflectionToString(value));
        });

        Assert.assertEquals(2, policies.size());

        policies.forEach((key, value) -> {
            LOG.info("Policies > {}: {}", key, ToStringBuilder.reflectionToString(value));
        });

        Assert.assertEquals(4, publishments.size());

        publishments.forEach((key, value) -> {
            LOG.info("Publishments > {}: {}", key, ToStringBuilder.reflectionToString(value));
        });
    }

    private StreamDefinition createStreamDefinitionWithNodataAlert() {
        StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("host");
        hostColumn.setType(StreamColumn.Type.STRING);
        hostColumn.setNodataExpression("PT1M,dynamic,1,host");

        StreamColumn valueColumn = new StreamColumn();
        valueColumn.setName("value");
        valueColumn.setType(StreamColumn.Type.DOUBLE);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, valueColumn));
        sd.setDataSource("testDataSource");
        sd.setStreamId("testStreamId");
        return sd;
    }

}
