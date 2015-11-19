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
package org.apache.eagle.query.aggregate.raw;

import junit.framework.Assert;

import org.junit.Test;

public class TestGroupbyKey {
	@Test
	public void testGroupbyKey(){
		GroupbyKey key1 = new GroupbyKey();
		Assert.assertEquals(0, key1.getValue().size());
		
		key1.addValue(new byte[]{1, 3, 5});
		Assert.assertEquals(1, key1.getValue().size());
		
		key1.clear();
		Assert.assertEquals(0, key1.getValue().size());
		
		key1.addValue(new byte[]{1, 3, 5});
		GroupbyKey key2 = new GroupbyKey();
		key2.addValue(new byte[]{1, 3, 5});
		Assert.assertEquals(key1, key2);
		
		GroupbyKey key3 = new GroupbyKey(key1);
		Assert.assertEquals(key1, key3);
		Assert.assertEquals(key2, key3);
	}
	
	@Test
	public void testGroupbyKeyComparator(){
		GroupbyKeyComparator comparator = new GroupbyKeyComparator();
		GroupbyKey key1 = new GroupbyKey();
		key1.addValue("hello".getBytes());
		GroupbyKey key2 = new GroupbyKey();
		key2.addValue("world".getBytes());
		int r = comparator.compare(key1, key2);
		Assert.assertTrue(r < 0);
		
		key2.clear();
		key2.addValue("friend".getBytes());
		r = comparator.compare(key1, key2);
		Assert.assertTrue(r > 0);
		
		key2.clear();
		key2.addValue("hello".getBytes());
		r = comparator.compare(key1, key2);
		Assert.assertTrue(r == 0);
		
		key1.clear();
		key2.clear();
		key1.addValue("hello".getBytes());
		key1.addValue("tom".getBytes());
		key2.addValue("hello".getBytes());
		key2.addValue("jackie".getBytes());
		r = comparator.compare(key1, key2);
		Assert.assertTrue(r > 0);
	}
}
