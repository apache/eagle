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
package org.apache.eagle.service.jackson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJacksonMarshalling {
	private static Logger LOG = LoggerFactory.getLogger(TestJacksonMarshalling.class);

	
	
	@Test
	public void testJSonArrayMarshalling(){
		String[] array = {"cluster", "datacenter", "rack", "hostname"};
		JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory);
	    String result = null;
	    try{
	    	result = mapper.writeValueAsString(array);
	    }catch(Exception ex){
	    	LOG.error("Cannot marshall", ex);
	    	Assert.fail("cannot marshall an String array");
	    }
	    Assert.assertEquals("[\"cluster\",\"datacenter\",\"rack\",\"hostname\"]", result);
	}
	

	static class Pojo{
		private String field1;
		private String field2;
		public String getField1() {
			return field1;
		}
		public void setField1(String field1) {
			this.field1 = field1;
		}
		public String getField2() {
			return field2;
		}
		public void setField2(String field2) {
			this.field2 = field2;
		}
	}
	
	@Test
	public void testPojoMarshalling(){
		Pojo p = new Pojo();
		p.setField1("field1");
		p.setField2("field2");
		
		JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory);
	    String result = null;
	    try{
	    	result = mapper.writeValueAsString(p);
	    }catch(Exception ex){
	    	LOG.error("Cannot marshall", ex);
	    	Assert.fail("Cannot marshall a Pojo");
	    }
	    System.out.println(result);
	    Assert.assertEquals("{\"field1\":\"field1\",\"field2\":\"field2\"}", result);
	}
	
	@Test
	public void testPojoArrayMashalling(){
		Pojo[] ps = new Pojo[2];
		ps[0] = new Pojo();
		ps[0].setField1("0_field1");
		ps[0].setField2("0_field2");
		ps[1] = new Pojo();
		ps[1].setField1("1_field1");
		ps[1].setField2("1_field2");
		
		JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory);
	    String result = null;
	    try{
	    	result = mapper.writeValueAsString(ps);
	    }catch(Exception ex){
	    	LOG.error("Cannot marshall", ex);
	    	Assert.fail("Cannot marshall a Pojo array");
	    }
	    System.out.println(result);
	    Assert.assertEquals("[{\"field1\":\"0_field1\",\"field2\":\"0_field2\"},{\"field1\":\"1_field1\",\"field2\":\"1_field2\"}]", result);
	}
	
	@Test
	public void testComplexMapMarshalling(){
		Map<List<String>, String> map = new HashMap<List<String>, String>();
		map.put(Arrays.asList("cluster1","dc1"), "123");
		map.put(Arrays.asList("cluster1","dc1"), "456");
		
		JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory);
	    String result = null;
	    try{
	    	result = mapper.writeValueAsString(map);
	    }catch(Exception ex){
	    	LOG.error("Cannot marshall", ex);
	    	Assert.fail("Cannot marshall a complex map");
	    }
	    System.out.println(result);
	}
	
	@Test
	public void testMapMapMarshalling(){
		Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
		Map<String, String> childmap1 = new HashMap<String, String>();
		childmap1.put("dc1", "123");
		childmap1.put("dc1", "456");
		map.put("cluster1", childmap1);
		
		JsonFactory factory = new JsonFactory(); 
	    ObjectMapper mapper = new ObjectMapper(factory);
	    String result = null;
	    try{
	    	result = mapper.writeValueAsString(map);
	    }catch(Exception ex){
	    	LOG.error("Cannot marshall", ex);
	    	Assert.fail("Cannot marshall a complex map");
	    }
	    System.out.println(result);
	}
}
