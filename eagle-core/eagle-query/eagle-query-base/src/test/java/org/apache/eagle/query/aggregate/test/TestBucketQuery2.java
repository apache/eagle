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
package org.apache.eagle.query.aggregate.test;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.query.aggregate.BucketQuery;
import junit.framework.Assert;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestBucketQuery2 {
	private static class SampleTaggedLogAPIEntity extends TaggedLogAPIEntity{
		private String description;

		@SuppressWarnings("unused")
		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}
	}

	@SuppressWarnings("unchecked")
//	@Test
	public void testBucketQuery(){
		SampleTaggedLogAPIEntity e1 = new SampleTaggedLogAPIEntity();
		e1.setTags(new HashMap<String, String>());
		e1.getTags().put("cluster", "cluster1");
		e1.getTags().put("rack", "rack123");
		e1.setDescription("this is description 1");
		
		SampleTaggedLogAPIEntity e2 = new SampleTaggedLogAPIEntity();
		e2.setTags(new HashMap<String, String>());
		e2.getTags().put("cluster", "cluster1");
		e2.getTags().put("rack", "rack123");
		e2.setDescription("this is description 2");
		
		List<String> bucketFields = new ArrayList<String>();
		bucketFields.add("cluster");
		int limit = 1;
		
		BucketQuery query1 = new BucketQuery(bucketFields, limit);
		query1.put(e1);
		query1.put(e2);
		
		Map<String, Object> map = query1.get();
		
		List<TaggedLogAPIEntity> o = (List<TaggedLogAPIEntity>)map.get("cluster1");
		Assert.assertEquals(limit, o.size());
		
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		try{
			String result = mapper.writeValueAsString(map);
			System.out.println(result);
		}catch(Exception ex){
			ex.printStackTrace();
			Assert.fail("can not serialize bucket query result");
		}
		
		limit = 2;
		BucketQuery query2 = new BucketQuery(bucketFields, limit);
		query2.put(e1);
		query2.put(e2);
		Map<String, Object> map2 = query2.get();
		o = (List<TaggedLogAPIEntity>)map2.get("cluster1");
		try{
			String result = mapper.writeValueAsString(map2);
			System.out.println(result);
		}catch(Exception ex){
			ex.printStackTrace();
			Assert.fail("can not serialize bucket query result");
		}
		Assert.assertEquals(limit, o.size());
		
		
		SampleTaggedLogAPIEntity e3 = new SampleTaggedLogAPIEntity();
		e3.setTags(new HashMap<String, String>());
		e3.getTags().put("cluster", "cluster1");
		e3.getTags().put("rack", "rack124");
		e3.setDescription("this is description 3");
		bucketFields.add("rack");
		limit = 2;
		BucketQuery query3 = new BucketQuery(bucketFields, limit);
		query3.put(e1);
		query3.put(e2);
		query3.put(e3);
		Map<String, Object> map3 = query3.get();
		Map<String, Object> o3 = (Map<String, Object>)map3.get("cluster1");
		List<TaggedLogAPIEntity> o4 = (List<TaggedLogAPIEntity>)o3.get("rack124");
		Assert.assertEquals(1, o4.size());
		List<TaggedLogAPIEntity> o5 = (List<TaggedLogAPIEntity>)o3.get("rack123");
		Assert.assertEquals(o5.size(), 2);
		
		try{
			String result = mapper.writeValueAsString(map3);
			System.out.println(result);
		}catch(Exception ex){
			ex.printStackTrace();
			Assert.fail("can not serialize bucket query result");
		}
		
		
		SampleTaggedLogAPIEntity e4 = new SampleTaggedLogAPIEntity();
		e4.setTags(new HashMap<String, String>());
		e4.getTags().put("cluster", "cluster1");
		// rack is set to null
//		e4.getTags().put("rack", "rack124");
		e4.setDescription("this is description 3");
		limit = 2;
		BucketQuery query4 = new BucketQuery(bucketFields, limit);
		query4.put(e1);
		query4.put(e2);
		query4.put(e3);
		query4.put(e4);
		Map<String, Object> map4 = query4.get();
		Map<String, Object> o6 = (Map<String, Object>)map4.get("cluster1");
		List<TaggedLogAPIEntity> o7 = (List<TaggedLogAPIEntity>)o6.get("rack124");
		Assert.assertEquals(1, o7.size());
		List<TaggedLogAPIEntity> o8 = (List<TaggedLogAPIEntity>)o6.get("rack123");
		Assert.assertEquals(o8.size(), 2);
		List<TaggedLogAPIEntity> o9 = (List<TaggedLogAPIEntity>)o6.get("unassigned");
		Assert.assertEquals(o9.size(), 1);
		
		try{
			String result = mapper.writeValueAsString(map4);
			System.out.println(result);
		}catch(Exception ex){
			ex.printStackTrace();
			Assert.fail("can not serialize bucket query result");
		}
	}

	@Test
	public void test() {

	}
}
