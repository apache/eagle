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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class TestListSerDeser {

	@SuppressWarnings("rawtypes")
	@Test
	public void testStringListSerDeser() {
		ListSerDeser serDeser = new ListSerDeser();
		List<String> sources = new ArrayList<String>();
		sources.add("value1");
		sources.add("value2");
		sources.add("value3");		
				
		byte[] bytes = serDeser.serialize(sources);
		Assert.assertEquals(4 + sources.size() * 8 + 18, bytes.length);
		List targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());
		
		Assert.assertTrue(targets.contains("value1"));
		Assert.assertTrue(targets.contains("value2"));
		Assert.assertTrue(targets.contains("value3"));
	}

	
	@SuppressWarnings("rawtypes")
	@Test
	public void testIntegerMapSerDeser() {
		ListSerDeser serDeser = new ListSerDeser();
		List<Integer> sources = new ArrayList<Integer>();
		sources.add(1);
		sources.add(2);
		sources.add(3);
		
		byte[] bytes = serDeser.serialize(sources);
		Assert.assertEquals(4 + sources.size() * 8 + 12, bytes.length);
		List targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());
		
		Assert.assertTrue(targets.contains(1));
		Assert.assertTrue(targets.contains(2));
		Assert.assertTrue(targets.contains(3));
	}

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testListListSerDeser() {
		ListSerDeser serDeser = new ListSerDeser();
		List<List<String>> sources = new ArrayList<List<String>>();
		List<String> list1 = new ArrayList<String>();
		list1.add("value1");
		list1.add("value2");
		list1.add("value3");
		sources.add(list1);
		
		List<String> list2 = new ArrayList<String>();
		list2.add("value4");
		list2.add("value5");		
		sources.add(list2);
		
		byte[] bytes = serDeser.serialize(sources);
		List targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.size(), targets.size());

		list1 = (List)targets.get(0);
		Assert.assertNotNull(list1);
		Assert.assertEquals(3, list1.size());
		Assert.assertTrue(list1.contains("value1"));
		Assert.assertTrue(list1.contains("value2"));
		Assert.assertTrue(list1.contains("value3"));

		list2 = (List)targets.get(1);
		Assert.assertNotNull(list2);
		Assert.assertEquals(2, list2.size());
		Assert.assertTrue(list2.contains("value4"));
		Assert.assertTrue(list2.contains("value5"));
	}
}
