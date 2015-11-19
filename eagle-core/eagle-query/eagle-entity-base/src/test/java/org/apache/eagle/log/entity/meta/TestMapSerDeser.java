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
package org.apache.eagle.log.entity.meta;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TestMapSerDeser {

	@SuppressWarnings("rawtypes")
	@Test
	public void testStringToStringMapSerDeser() {
		MapSerDeser serDeser = new MapSerDeser();
		Map<String, String> sources = new HashMap<String, String>();
		sources.put("test1", "value1");
		sources.put("test2", null);
		sources.put("test3", "value3");
		
		byte[] bytes = serDeser.serialize(sources);
		Assert.assertEquals(4 + sources.size() * 16 + 27, bytes.length);
		Map targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());
		
		Assert.assertEquals("value1", targets.get("test1"));
		Assert.assertNull(targets.get("test2"));
		Assert.assertEquals("value3", targets.get("test3"));
	}

	
	@SuppressWarnings("rawtypes")
	@Test
	public void testStringToIntegerMapSerDeser() {
		MapSerDeser serDeser = new MapSerDeser();
		Map<String, Integer> sources = new HashMap<String, Integer>();
		sources.put("test1", 1);
		sources.put("test2", null);
		sources.put("test3", 3);
		
		byte[] bytes = serDeser.serialize(sources);
		Assert.assertEquals(4 + sources.size() * 16 + 23, bytes.length);
		Map targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());
		
		Assert.assertEquals(1, targets.get("test1"));
		Assert.assertNull(targets.get("test2"));
		Assert.assertEquals(3, targets.get("test3"));
	}

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testStringToMapMapSerDeser() {
		MapSerDeser serDeser = new MapSerDeser();
		Map<String, Map<String, String>> sources = new HashMap<String, Map<String, String>>();
		Map<String, String> map1 = new HashMap<String, String>();
		map1.put("key11", "value11");
		map1.put("key12", null);
		map1.put("key13", "value13");
		sources.put("test1", map1);
		sources.put("test2", null);
		Map<String, String> map3 = new HashMap<String, String>();
		map3.put("key31", "value31");
		map3.put("key32", null);
		map3.put("key33", "value33");
		sources.put("test3", map3);
		
		byte[] bytes = serDeser.serialize(sources);
		Map targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());

		map1 = (Map)targets.get("test1");
		Assert.assertNotNull(map1);
		Assert.assertEquals(3, map1.size());
		Assert.assertEquals("value11", map1.get("key11"));
		Assert.assertNull(map1.get("key12"));
		Assert.assertEquals("value13", map1.get("key13"));
		
		Assert.assertNull(targets.get("test2"));
		
		map3 = (Map)targets.get("test3");
		Assert.assertNotNull(map3);
		Assert.assertEquals(3, map3.size());
		Assert.assertEquals("value31", map3.get("key31"));
		Assert.assertNull(map3.get("key32"));
		Assert.assertEquals("value33", map3.get("key33"));
	}

}
