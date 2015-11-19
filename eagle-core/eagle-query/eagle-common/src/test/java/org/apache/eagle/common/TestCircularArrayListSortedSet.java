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

public class TestCircularArrayListSortedSet {

	@Test
	public void testInsertAndRemove() {
		Long[] array = new Long[5];
		CircularArrayListSortedSet<Long> set = new CircularArrayListSortedSet<Long>(array);
		
		set.insert(3L);
		set.insert(2L);
		set.insert(1L);
		set.insert(5L);
		set.insert(4L);
		
		for (int i = 0; i < 5; ++i) {
			Assert.assertEquals((Long)(long)(i + 1),set.get(i));
			Assert.assertEquals(i, set.binarySearch((Long)(long)(i + 1)));
		}
		Assert.assertEquals(0, set.head());
		Assert.assertEquals(0, set.tail());
		Assert.assertTrue(set.isFull());
		Assert.assertEquals(-(5 + 1), set.binarySearch(6L));
		
		Assert.assertEquals(2, set.remove(3L));
		Assert.assertEquals(2, set.remove(4L));
		Assert.assertEquals(-(2 + 1), set.binarySearch(3L));
		set.insert(3L);
		set.insert(4L);
		
		for (int i = 0; i < 5; ++i) {
			Assert.assertEquals((Long)(long)(i + 1),set.get(i));
			Assert.assertEquals(i, set.binarySearch((Long)(long)(i + 1)));
		}
		Assert.assertEquals(2, set.head());
		Assert.assertEquals(2, set.tail());
		Assert.assertTrue(set.isFull());
	}
}
