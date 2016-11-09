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

package org.apache.eagle.alert.engine.coordinator;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PublishmentTest {
    @Test
    public void testPublishment() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("kafka_broker", "localhost:9092");
        properties.put("topic", "TEST_TOPIC_NAME");

        List<Map<String, Object>> kafkaClientConfig = new ArrayList<>();
        kafkaClientConfig.add(ImmutableMap.of("name", "producer.type", "value", "sync"));
        properties.put("kafka_client_config", kafkaClientConfig);

        PolicyDefinition policy = createPolicy("testStream", "testPolicy");
        Publishment publishment = new Publishment();
        publishment.setName("testAsyncPublishment");
        publishment.setType("org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher");
        publishment.setPolicyIds(Arrays.asList(policy.getName()));
        publishment.setDedupIntervalMin("PT0M");
        OverrideDeduplicatorSpec overrideDeduplicatorSpec = new OverrideDeduplicatorSpec();
        overrideDeduplicatorSpec.setClassName("testClass");
        publishment.setOverrideDeduplicator(overrideDeduplicatorSpec);
        publishment.setSerializer("org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer");
        publishment.setProperties(properties);

        Assert.assertEquals("Publishment[name:testAsyncPublishment,type:org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher,policyId:[testPolicy],properties:{kafka_client_config=[{name=producer.type, value=sync}], topic=TEST_TOPIC_NAME, kafka_broker=localhost:9092}", publishment.toString());


        Publishment publishment1 = new Publishment();
        publishment1.setName("testAsyncPublishment");
        publishment1.setType("org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher");
        publishment1.setPolicyIds(Arrays.asList(policy.getName()));
        publishment1.setDedupIntervalMin("PT0M");
        OverrideDeduplicatorSpec overrideDeduplicatorSpec1 = new OverrideDeduplicatorSpec();
        overrideDeduplicatorSpec1.setClassName("testClass");
        publishment1.setOverrideDeduplicator(overrideDeduplicatorSpec1);
        publishment1.setSerializer("org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer");
        publishment1.setProperties(properties);

        Assert.assertTrue(publishment.equals(publishment1));
        Assert.assertTrue(publishment.hashCode() == publishment1.hashCode());
        Assert.assertFalse(publishment == publishment1);
        publishment1.getOverrideDeduplicator().setClassName("testClass1");


        Assert.assertFalse(publishment.equals(publishment1));
        Assert.assertFalse(publishment.hashCode() == publishment1.hashCode());
        Assert.assertFalse(publishment == publishment1);

        publishment1.getOverrideDeduplicator().setClassName("testClass");
        publishment1.setStreamIds(Arrays.asList("streamid1,streamid2"));
        Assert.assertFalse(publishment.equals(publishment1));
        Assert.assertFalse(publishment.hashCode() == publishment1.hashCode());
        Assert.assertFalse(publishment == publishment1);
    }

    private PolicyDefinition createPolicy(String streamName, String policyName) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        // expression, something like "PT5S,dynamic,1,host"
        def.setValue("test");
        def.setType("siddhi");
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList("inputStream"));
        pd.setOutputStreams(Arrays.asList("outputStream"));
        pd.setName(policyName);
        pd.setDescription(String.format("Test policy for stream %s", streamName));

        StreamPartition sp = new StreamPartition();
        sp.setStreamId(streamName);
        sp.setColumns(Arrays.asList("host"));
        sp.setType(StreamPartition.Type.GROUPBY);
        pd.addPartition(sp);
        return pd;
    }

}
