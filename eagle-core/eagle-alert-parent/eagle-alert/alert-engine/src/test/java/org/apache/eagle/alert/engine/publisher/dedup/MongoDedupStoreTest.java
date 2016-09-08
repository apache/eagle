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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.junit.Assert;
import org.junit.Test;

public class MongoDedupStoreTest extends MongoDependencyBaseTest {

	@Test
    public void testNormal() throws Exception {
		Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> events = store.getEvents();
		Assert.assertNotNull(events);
		Assert.assertEquals(0, events.size());
		
		String streamId = "testStream"; 
		String policyId = "testPolicy"; 
		long timestamp = System.currentTimeMillis(); 
		HashMap<String, String> customFieldValues = new HashMap<String, String>();
		customFieldValues.put("alertKey", "test-alert-key");
		EventUniq eventEniq = new EventUniq(streamId, policyId, timestamp, customFieldValues);
		
		ConcurrentLinkedDeque<DedupValue> dedupStateValues = new ConcurrentLinkedDeque<DedupValue>();
		DedupValue one = new DedupValue();
		one.setStateFieldValue("OPEN");
		one.setCount(2);
		one.setFirstOccurrence(System.currentTimeMillis());
		dedupStateValues.add(one);
		store.add(eventEniq, dedupStateValues);
		
		events = store.getEvents();
		Assert.assertNotNull(events);
		Assert.assertEquals(1, events.size());
		
		Entry<EventUniq, ConcurrentLinkedDeque<DedupValue>> entry = events.entrySet().iterator().next();
		Assert.assertEquals(streamId, entry.getKey().streamId);
		Assert.assertEquals(1, entry.getValue().size());
		Assert.assertEquals(2, entry.getValue().getLast().getCount());
	}
    
}
