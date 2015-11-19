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
package org.apache.eagle.service.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

// http://wiki.fasterxml.com/JacksonPolymorphicDeserialization
public class TestJackson {

	@Test
	public void testBase() throws JsonGenerationException, JsonMappingException, IOException {
		List<Base> objs = new ArrayList<Base>();
		ClassA a = new ClassA();
		a.setA(1);
		ClassB b = new ClassB();
		b.setB("2");
		
		objs.add(a);
		objs.add(b);
		
		ObjectMapper om = new ObjectMapper();
		om.enableDefaultTyping();
//		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		String value = om.writeValueAsString(objs);
		
		System.out.println("value = " + value);
		
		@SuppressWarnings("rawtypes")
		List result = om.readValue(value, ArrayList.class);
		System.out.println("size = " + result.size());
		Object obj1 = result.get(0);
		Object obj2 = result.get(1);
		
		Assert.assertEquals("ClassA", obj1.getClass().getSimpleName());
		Assert.assertEquals(1, ((ClassA)obj1).getA());
		Assert.assertEquals("ClassB", obj2.getClass().getSimpleName());
		Assert.assertEquals("2", ((ClassB)obj2).getB());
		
	}
}
