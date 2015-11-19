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

import junit.framework.Assert;

import org.junit.Test;

public class TestArraySerDeser {
	
	@Test
	public void testIntArraySerDeser(){
		IntArraySerDeser serDeser = new IntArraySerDeser();
		int[] ints = new int[] {1, 34, 21, 82};
		byte[] bytes = serDeser.serialize(ints);
		Assert.assertEquals((ints.length+1)*4, bytes.length);
		int[] targets = serDeser.deserialize(bytes);
		Assert.assertEquals(ints.length, targets.length);
		for(int i=0; i<ints.length; i++){
			Assert.assertEquals(ints[i], targets[i]);
		}
	}
	
	@Test
	public void testDoubleArraySerDeser(){
		DoubleArraySerDeser serDeser = new DoubleArraySerDeser();
		double[] doubles = new double[] {1.0, 34.0, 21.0, 82.0};
		byte[] bytes = serDeser.serialize(doubles);
		Assert.assertEquals(4 + doubles.length*8, bytes.length);
		double[] targets = serDeser.deserialize(bytes);
		Assert.assertEquals(doubles.length, targets.length);
		for(int i=0; i<doubles.length; i++){
			Assert.assertEquals(doubles[i], targets[i]);
		}
	}

	@Test
	public void testStringArraySerDeser(){
		StringArraySerDeser serDeser = new StringArraySerDeser();
		String[] sources = new String[] {"a", "", "1", "2", "3"};
		byte[] bytes = serDeser.serialize(sources);
		Assert.assertEquals(4 + sources.length*4 + 4, bytes.length);
		String[] targets = serDeser.deserialize(bytes);
		Assert.assertEquals(sources.length, targets.length);
		for(int i=0; i<sources.length; i++){
			Assert.assertEquals(sources[i], targets[i]);
		}
	}

}
