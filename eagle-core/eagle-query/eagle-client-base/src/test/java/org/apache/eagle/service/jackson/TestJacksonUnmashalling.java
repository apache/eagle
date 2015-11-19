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

import java.io.File;
import java.util.Map;

import junit.framework.Assert;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJacksonUnmashalling {
	private static Logger LOG = LoggerFactory.getLogger(TestJacksonUnmashalling.class);
	private static File arrayJson;
	private static File mapJson;
	private static File pojoJson;
	private static File pojoArrayJson;
	@SuppressWarnings("unused")
	private static File complexPojoJson;
	
	@BeforeClass
	public static void prepare(){ 
		try{
			arrayJson = new File(TestJacksonUnmashalling.class.getResource("/arrayJson.txt").getPath());
			mapJson = new File(TestJacksonUnmashalling.class.getResource("/mapJson.txt").getPath());
			pojoJson = new File(TestJacksonUnmashalling.class.getResource("/pojoJson.txt").getPath());
			pojoArrayJson = new File(TestJacksonUnmashalling.class.getResource("/pojoArrayJson.txt").getPath());
			complexPojoJson = new File(TestJacksonUnmashalling.class.getResource("/complexPojoJson.txt").getPath());
		}catch(Exception ex){
			LOG.error("Cannot read json files", ex);
			Assert.fail("Cannot read json files");
		}
	}
	
	@AfterClass
	public static void end(){
	}
	
	@Test
	public void testArrayJsonUnmashalling(){
		try{
			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<String[]> type = new TypeReference<String[]>() {};
			String[] array = mapper.readValue(arrayJson, type);
			StringBuffer sb = new StringBuffer();
			for(String str : array){
				sb.append(str);
				sb.append(",");
			}
			LOG.info(sb.toString());
		}catch(Exception ex){
			LOG.error("Cannot unmashall an array json file", ex);
			Assert.fail("Cannot unmashall an array json file arrayJson.txt");
		}
	}
	
	@Test
	public void testMapJsonUnmashalling(){
		try{
			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<Map<String, String>> type = new TypeReference<Map<String, String>>() {};
			Map<String, String> map = mapper.readValue(mapJson, type);
			StringBuffer sb = new StringBuffer();
			for(Map.Entry<String, String> entry : map.entrySet()){
				sb.append(entry.getKey());
				sb.append(":");
				sb.append(entry.getValue());
				sb.append(",");
			}
			LOG.info(sb.toString());
		}catch(Exception ex){
			LOG.error("Cannot unmashall a map json file", ex);
			Assert.fail("Cannot unmashall a map json file arrayJson.txt");
		}
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
	public void testPojoJsonUnmashalling(){
		try{
			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<Pojo> type = new TypeReference<Pojo>() {};
			Pojo p = mapper.readValue(pojoJson, type);
			LOG.info(p.getField1() + "," + p.getField2());
		}catch(Exception ex){
			LOG.error("Cannot unmashall a map json file", ex);
			Assert.fail("Cannot unmashall a map json file arrayJson.txt");
		}
	}
	
	@Test
	public void testPojoArrayJsonUnmashalling(){
		try{
			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			TypeReference<Pojo[]> type = new TypeReference<Pojo[]>() {};
			Pojo[] ps = mapper.readValue(pojoArrayJson, type);
			for(Pojo p : ps){
				LOG.info(p.getField1() + "," + p.getField2());
			}
		}catch(Exception ex){
			LOG.error("Cannot unmashall a map json file", ex);
			Assert.fail("Cannot unmashall a map json file arrayJson.txt");
		}
	}
	
}


