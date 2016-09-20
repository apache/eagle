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
import org.apache.eagle.alert.engine.publisher.dedup.DedupEventsStore;
import org.apache.eagle.alert.engine.publisher.dedup.DedupEventsStoreFactory;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublishPluginsFactory;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.runner.AlertPublisherBolt;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Since 5/14/16.
 */
public class TestAlertPublisherBolt {
	
	private DedupEventsStore store;
	
	@Before
	public void setUp() {
		store = Mockito.mock(DedupEventsStore.class);
    	DedupEventsStoreFactory.customizeStore(store);
	}
	
	@After
	public void tearDown() {
        Mockito.reset(store);
	}

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

    private AlertStreamEvent create(String streamId) {
        AlertStreamEvent alert = new AlertStreamEvent();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("policy1");
        alert.setPolicyId(policy.getName());
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[]{"field_1", 2, "field_3"});
        alert.setStreamId(streamId);
        alert.setCreatedBy(this.toString());
        return alert;
    }


    @Test
    public void testMapComparatorAdded() {

        PublishSpec spec1 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForAdd1.json"), PublishSpec.class);
        PublishSpec spec2 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForAdd0.json"), PublishSpec.class);

        Map<String, Publishment> map1 = new HashMap<>();
        Map<String, Publishment> map2 = new HashMap<>();
        spec1.getPublishments().forEach(p -> map1.put(p.getName(), p));
        spec2.getPublishments().forEach(p -> map2.put(p.getName(), p));

        MapComparator<String, Publishment> comparator = new MapComparator<>(map1, map2);
        comparator.compare();
        Assert.assertTrue(comparator.getAdded().size() == 1);

    }

    @Test
    public void testMapComparatorRemoved() {

        PublishSpec spec1 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForAdd0.json"), PublishSpec.class);
        PublishSpec spec2 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForAdd1.json"), PublishSpec.class);

        Map<String, Publishment> map1 = new HashMap<>();
        Map<String, Publishment> map2 = new HashMap<>();
        spec1.getPublishments().forEach(p -> map1.put(p.getName(), p));
        spec2.getPublishments().forEach(p -> map2.put(p.getName(), p));

        MapComparator<String, Publishment> comparator = new MapComparator<>(map1, map2);
        comparator.compare();
        Assert.assertTrue(comparator.getRemoved().size() == 1);

    }

    @Test
    public void testMapComparatorModified() {

        PublishSpec spec1 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForAdd0.json"), PublishSpec.class);
        PublishSpec spec2 = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishForMdyValue.json"), PublishSpec.class);

        Map<String, Publishment> map1 = new HashMap<>();
        Map<String, Publishment> map2 = new HashMap<>();
        spec1.getPublishments().forEach(p -> map1.put(p.getName(), p));
        spec2.getPublishments().forEach(p -> map2.put(p.getName(), p));

        MapComparator<String, Publishment> comparator = new MapComparator<>(map1, map2);
        comparator.compare();
        Assert.assertTrue(comparator.getModified().size() == 1);

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

    private AlertStreamEvent createWithStreamDef(String hostname, String appName, String state) {
        AlertStreamEvent alert = new AlertStreamEvent();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("perfmon_cpu_host_check");
        alert.setPolicyId(policy.getName());
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[] {appName, hostname, state});
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
        
        StreamColumn stateColumn = new StreamColumn();
        stateColumn.setName("state");
        stateColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(appColumn, hostColumn, stateColumn));

        alert.setSchema(sd);
        return alert;
    }

	@Test
    public void testCustomFieldDedupEvent() throws Exception {
        List<Publishment> pubs = loadEntities("/router/publishments.json", Publishment.class);

        AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(pubs.get(0), null, null);
        AlertStreamEvent event1 = createWithStreamDef("host1", "testapp1", "OPEN");
        AlertStreamEvent event2 = createWithStreamDef("host2", "testapp1", "OPEN");
        AlertStreamEvent event3 = createWithStreamDef("host2", "testapp2", "CLOSE");

        Assert.assertNotNull(plugin.dedup(event1));
        Assert.assertNull(plugin.dedup(event2));
        Assert.assertNotNull(plugin.dedup(event3));
        
        Mockito.verify(store).getEvents();
        Mockito.verify(store, Mockito.atLeastOnce()).add(Mockito.anyObject(), Mockito.anyObject());
    }

	@Test
    public void testEmptyCustomFieldDedupEvent() throws Exception {
        List<Publishment> pubs = loadEntities("/router/publishments-empty-dedup-field.json", Publishment.class);

        AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(pubs.get(0), null, null);
        AlertStreamEvent event1 = createWithStreamDef("host1", "testapp1", "OPEN");
        AlertStreamEvent event2 = createWithStreamDef("host1", "testapp1", "OPEN");

        Assert.assertNotNull(plugin.dedup(event1));
        Assert.assertNull(plugin.dedup(event2));
        
        Mockito.verify(store, Mockito.atLeastOnce()).add(Mockito.anyObject(), Mockito.anyObject());
    }

    private AlertStreamEvent createSeverityWithStreamDef(String hostname, String appName, String message, String severity, String docId, String df_device, String df_type, String colo) {
        AlertStreamEvent alert = new AlertStreamEvent();
        PolicyDefinition policy = new PolicyDefinition();
        policy.setName("switch_check");
        alert.setPolicyId(policy.getName());
        alert.setCreatedTime(System.currentTimeMillis());
        alert.setData(new Object[] {appName, hostname, message, severity, docId, df_device, df_type, colo});
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

        StreamColumn msgColumn = new StreamColumn();
        msgColumn.setName("message");
        msgColumn.setType(StreamColumn.Type.STRING);

        StreamColumn severityColumn = new StreamColumn();
        severityColumn.setName("severity");
        severityColumn.setType(StreamColumn.Type.STRING);

        StreamColumn docIdColumn = new StreamColumn();
        docIdColumn.setName("docId");
        docIdColumn.setType(StreamColumn.Type.STRING);

        StreamColumn deviceColumn = new StreamColumn();
        deviceColumn.setName("df_device");
        deviceColumn.setType(StreamColumn.Type.STRING);

        StreamColumn deviceTypeColumn = new StreamColumn();
        deviceTypeColumn.setName("df_type");
        deviceTypeColumn.setType(StreamColumn.Type.STRING);

        StreamColumn coloColumn = new StreamColumn();
        coloColumn.setName("dc");
        coloColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(appColumn, hostColumn, msgColumn, severityColumn, docIdColumn, deviceColumn, deviceTypeColumn, coloColumn));

        alert.setSchema(sd);
        return alert;
    }

    @Test
    public void testSlackPublishment() throws Exception {
        Config config = ConfigFactory.load("application-test.conf");
        AlertPublisher publisher = new AlertPublisherImpl("alertPublishBolt");
        publisher.init(config, new HashMap());
        List<Publishment> pubs = loadEntities("/router/publishments-slack.json", Publishment.class);
        publisher.onPublishChange(pubs, null, null, null);

        AlertStreamEvent event1 = createSeverityWithStreamDef("switch1", "testapp1", "Memory 1 inconsistency detected", "WARNING", "docId1", "ed01", "distribution switch", "us");
        AlertStreamEvent event2 = createSeverityWithStreamDef("switch2", "testapp2", "Memory 2 inconsistency detected", "CRITICAL", "docId2", "ed02", "distribution switch", "us");
        AlertStreamEvent event3 = createSeverityWithStreamDef("switch2", "testapp2", "Memory 3 inconsistency detected", "WARNING", "docId3", "ed02", "distribution switch", "us");

        publisher.nextEvent(event1);
        publisher.nextEvent(event2);
        publisher.nextEvent(event3);

    }
}
