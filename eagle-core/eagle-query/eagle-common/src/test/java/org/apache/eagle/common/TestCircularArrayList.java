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
package org.apache.eagle.common;

import junit.framework.Assert;

import org.junit.Test;

public class TestCircularArrayList {

	@Test
	public void testAddAndRemove() {
		Long[] array = new Long[5];
		CircularArrayList<Long> list = new CircularArrayList<Long>(array);
		
		for (long i = 0 ; i < 5; ++i) {
			list.add(i);
			Assert.assertEquals((Long)i, array[(int) i]);
			Assert.assertTrue(list.contains(i));
			Assert.assertEquals(i, list.find(i));
		}
		Assert.assertFalse(list.contains(6L));
		Exception e = null;
		try {
			list.add((long)5);
		} catch (Exception ex) {
			e = ex;
		}
		Assert.assertNotNull(e);
		Assert.assertEquals(0, list.tail());
		Assert.assertEquals(0, list.head());
		Assert.assertEquals(5, list.size());
		Assert.assertTrue(list.isFull());
		Long v = list.remove(1);
		Assert.assertEquals((Long)1L, v);
		Assert.assertEquals(4, list.size());
		Assert.assertEquals(1, list.head());
		Assert.assertEquals(0, list.tail());
		list.add((long)5);
		Assert.assertEquals(5, list.size());
		Assert.assertEquals(1, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertEquals((Long)0L, list.remove(0));
		Assert.assertEquals(2, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertEquals((Long)2L, list.remove(0));
		Assert.assertEquals(3, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertEquals((Long)3L, list.remove(0));
		Assert.assertEquals(4, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertEquals((Long)4L, list.remove(0));
		Assert.assertEquals(0, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertEquals((Long)5L, list.remove(0));
		Assert.assertEquals(1, list.head());
		Assert.assertEquals(1, list.tail());
		Assert.assertTrue(list.isEmpty());
		Assert.assertFalse(list.isFull());
	}
}
