/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.router;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublishPluginsFactory;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.runner.AlertPublisherBolt;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.apache.eagle.alert.utils.MapComparator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Since 5/14/16.
 */
public class TestAlertPublisherBolt {

    @SuppressWarnings("rawtypes")
    @Ignore
    @Test
    public void test() {
        Config config = ConfigFactory.load("application-test.conf");
        AlertPublisher publisher = new AlertPublisherImpl("alertPublishBolt");
        publisher.init(config, new HashMap());
        PublishSpec spec = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishSpec.json"), PublishSpec.class);
        publisher.onPublishChange(spec.getPublishments(), null, null, null);
        AlertStreamEvent event = create("testAlertStream");
        publisher.nextEvent(event);
        AlertStreamEvent event1 = create("testAlertStream");
        publisher.nextEvent(event1);
    }

    private AlertStreamEvent create(String streamId){
        AlertStreamEvent alert = new AlertStreamEvent();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("policy1");
        alert.setPolicy(policy);
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[]{"field_1", 2, "field_3"});
        alert.setStreamId(streamId);
        alert.setCreatedBy(this.toString());
        return alert;
    }

    @Test
    public void testMapComparator() {
        PublishSpec spec1 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishSpec.json"), PublishSpec.class);
        PublishSpec spec2 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishSpec2.json"), PublishSpec.class);
        Map<String, Publishment> map1 = new HashMap<>();
        Map<String, Publishment> map2 = new HashMap<>();
        spec1.getPublishments().forEach(p -> map1.put(p.getName(), p));
        spec2.getPublishments().forEach(p -> map2.put(p.getName(), p));

        MapComparator<String, Publishment> comparator = new MapComparator<>(map1, map2);
        comparator.compare();
        Assert.assertTrue(comparator.getModified().size() == 1);

        AlertPublisherBolt publisherBolt = new AlertPublisherBolt("alert-publisher-test", null, null);
        publisherBolt.onAlertPublishSpecChange(spec1, null);
        publisherBolt.onAlertPublishSpecChange(spec2, null);
    }

    @Test
    public void testAlertPublisher() throws Exception {
        AlertPublisher alertPublisher = new AlertPublisherImpl("alert-publisher-test");
        List<Publishment> oldPubs = loadEntities("/publishments1.json", Publishment.class);
        List<Publishment> newPubs = loadEntities("/publishments2.json", Publishment.class);
        alertPublisher.onPublishChange(oldPubs, null, null, null);
        alertPublisher.onPublishChange(null, null, newPubs, oldPubs);
    }

    private <T> List<T> loadEntities(String path, Class<T> tClz) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JavaType type = CollectionType.construct(List.class, SimpleType.construct(tClz));
        List<T> l = objectMapper.readValue(TestAlertPublisherBolt.class.getResourceAsStream(path), type);
        return l;
    }

    private AlertStreamEvent createWithStreamDef(String hostname, String appName){
        AlertStreamEvent alert = new AlertStreamEvent();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("perfmon_cpu_host_check");
        alert.setPolicy(policy);
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[]{appName, hostname});
        alert.setStreamId("testAlertStream");
        alert.setCreatedBy(this.toString());

        // build stream definition
        StreamDefinition sd = new StreamDefinition();
        StreamColumn appColumn = new StreamColumn();
        appColumn.setName("appname");
        appColumn.setType(StreamColumn.Type.STRING);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("hostname");
        hostColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(appColumn, hostColumn));

        alert.setSchema(sd);
        return alert;
    }

    @Test
    public void testCustomFieldDedupEvent() throws Exception {
        List<Publishment> pubs = loadEntities("/router/publishments.json", Publishment.class);

        AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(pubs.get(0), null, null);
        AlertStreamEvent event1 = createWithStreamDef("host1", "testapp1");
        AlertStreamEvent event2 = createWithStreamDef("host2", "testapp1");
        AlertStreamEvent event3 = createWithStreamDef("host2", "testapp2");

        Assert.assertNotNull(plugin.dedup(event1));
        Assert.assertNull(plugin.dedup(event2));
        Assert.assertNotNull(plugin.dedup(event3));

    }

    @Test
    public void testEmptyCustomFieldDedupEvent() throws Exception {
        List<Publishment> pubs = loadEntities("/router/publishments-empty-dedup-field.json", Publishment.class);

        AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(pubs.get(0), null, null);
        AlertStreamEvent event1 = createWithStreamDef("host1", "testapp1");
        AlertStreamEvent event2 = createWithStreamDef("host2", "testapp2");

        Assert.assertNotNull(plugin.dedup(event1));
        Assert.assertNull(plugin.dedup(event2));

    }
}
