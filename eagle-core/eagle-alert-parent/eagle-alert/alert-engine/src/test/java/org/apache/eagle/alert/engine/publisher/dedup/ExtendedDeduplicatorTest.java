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
package org.apache.eagle.alert.engine.publisher.dedup;

import java.util.Arrays;
import java.util.List;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublishPluginsFactory;
import org.apache.eagle.alert.engine.router.TestAlertPublisherBolt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;

public class ExtendedDeduplicatorTest {

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
	
	@Test
	public void testNormal() throws Exception {
		List<Publishment> pubs = loadEntities("/router/publishments-extended-deduplicator.json", Publishment.class);

        AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(pubs.get(0), null, null);
        AlertStreamEvent event1 = createWithStreamDef("extended_dedup_host1", "extended_dedup_testapp1", "OPEN");
        AlertStreamEvent event2 = createWithStreamDef("extended_dedup_host2", "extended_dedup_testapp1", "OPEN");
        AlertStreamEvent event3 = createWithStreamDef("extended_dedup_host2", "extended_dedup_testapp2", "CLOSE");

        Assert.assertNotNull(plugin.dedup(event1));
        Assert.assertNull(plugin.dedup(event2));
        Assert.assertNotNull(plugin.dedup(event3));
        
        Mockito.verify(store, Mockito.atLeastOnce()).add(Mockito.anyObject(), Mockito.anyObject());
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
	
}
