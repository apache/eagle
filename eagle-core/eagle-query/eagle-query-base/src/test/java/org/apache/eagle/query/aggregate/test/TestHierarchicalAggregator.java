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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.eagle.query.aggregate.timeseries.PostHierarchicalAggregateSort;
import org.apache.eagle.query.aggregate.timeseries.HierarchicalAggregateEntity;
import org.apache.eagle.query.aggregate.timeseries.HierarchicalAggregator;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import junit.framework.Assert;

import org.apache.eagle.query.aggregate.timeseries.SortOption;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.test.TestEntity;


public class TestHierarchicalAggregator {
private final static Logger LOG = LoggerFactory.getLogger(TestHierarchicalAggregator.class);

	@SuppressWarnings("serial")
	private TestEntity createEntity(final String cluster, final String datacenter, final String rack, int numHosts, long numClusters){
		TestEntity entity = new TestEntity();
		Map<String, String> tags = new HashMap<String, String>(){{
			put("cluster", cluster);
			put("datacenter", datacenter);
			put("rack", rack);
		}}; 
		entity.setTags(tags);
		entity.setNumHosts(numHosts);
		entity.setNumClusters(numClusters);
		return entity;
	}
	
	@SuppressWarnings("serial")
	private TestEntity createEntityWithoutDatacenter(final String cluster, final String rack, int numHosts, long numClusters){
		TestEntity entity = new TestEntity();
		Map<String, String> tags = new HashMap<String, String>(){{
			put("cluster", cluster);
			put("rack", rack);
		}}; 
		entity.setTags(tags);
		entity.setNumHosts(numHosts);
		entity.setNumClusters(numClusters);
		return entity;
	}

	private void writeToJson(String message, Object obj){
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		try{
			String result = mapper.writeValueAsString(obj);
			LOG.info(message + ":\n" + result);
		}catch(Exception ex){
			LOG.error("Can not write json", ex);
			Assert.fail("Can not write json");
		}
	}
	
	@Test
	public void testZeroGropubyFieldHierarchicalAggregator(){ 
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		HierarchicalAggregator agg = new HierarchicalAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate", result);
			Assert.assertEquals(result.getChildren().size(), 0);
			Assert.assertEquals(result.getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));

			// test sort by function1
			SortOption so = new SortOption();
			so.setIndex(0);
			so.setAscendant(true);
			List<SortOption> sortOptions = Arrays.asList(so);
			PostHierarchicalAggregateSort.sort(result, sortOptions);
			writeToJson("After sort" ,result);
			Assert.assertEquals(null, result.getChildren());
			Assert.assertEquals(0, result.getSortedList().size());
			Assert.assertEquals(result.getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testSingleGropubyFieldHierarchicalAggregator(){ 
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc2", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		HierarchicalAggregator agg = new HierarchicalAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate" ,result);
			Assert.assertEquals(result.getChildren().size(), 2);
			Assert.assertEquals(result.getChildren().get("cluster1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster2").getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			
			// test sort by function 1
			SortOption so = new SortOption();
			so.setIndex(0);
			so.setAscendant(true);
			List<SortOption> sortOptions = Arrays.asList(so);
			PostHierarchicalAggregateSort.sort(result, sortOptions);
			writeToJson("After sort" ,result);
			Assert.assertEquals(null, result.getChildren());
			Assert.assertEquals(2, result.getSortedList().size(), 2);
			Iterator<Map.Entry<String, HierarchicalAggregateEntity>> it = result.getSortedList().iterator();
			Assert.assertEquals(true, it.hasNext());
			Map.Entry<String, HierarchicalAggregateEntity> entry = it.next();
			Assert.assertEquals("cluster2", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			
			Assert.assertEquals(true, it.hasNext());
			entry = it.next();
			Assert.assertEquals("cluster1", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new HierarchicalAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate" , result);
			Assert.assertEquals(result.getChildren().size(), 2);
			Assert.assertEquals(result.getChildren().get("dc1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("dc2").getValues().get(0), (double)(entities[2].getNumHosts()));
			
			// test sort by function 1
			SortOption so = new SortOption();
			so.setIndex(0);
			so.setAscendant(true);
			List<SortOption> sortOptions = Arrays.asList(so);
			PostHierarchicalAggregateSort.sort(result, sortOptions);
			writeToJson("After sort" ,result);
			Assert.assertEquals(null, result.getChildren());
			Assert.assertEquals(2, result.getSortedList().size(), 2);
			Iterator<Map.Entry<String, HierarchicalAggregateEntity>> it = result.getSortedList().iterator();
			Assert.assertEquals(true, it.hasNext());
			Map.Entry<String, HierarchicalAggregateEntity> entry = it.next();
			Assert.assertEquals("dc2", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[2].getNumHosts()));
			
			Assert.assertEquals(true, it.hasNext());
			entry = it.next();
			Assert.assertEquals("dc1", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));			
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new HierarchicalAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum, AggregateFunctionType.sum), Arrays.asList("numHosts", "numClusters"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate" , result);
			Assert.assertEquals(result.getChildren().size(), 2);
			Assert.assertEquals(2, result.getChildren().get("cluster1").getValues().size());
			Assert.assertEquals(result.getChildren().get("cluster1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster1").getValues().get(1), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+entities[2].getNumClusters()));
			Assert.assertEquals(2, result.getChildren().get("cluster2").getValues().size());
			Assert.assertEquals(result.getChildren().get("cluster2").getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster2").getValues().get(1), (double)(entities[3].getNumClusters()+entities[4].getNumClusters()));
			
			// test sort by function 2
			SortOption so = new SortOption();
			so.setIndex(1);
			so.setAscendant(true);
			List<SortOption> sortOptions = Arrays.asList(so);
			PostHierarchicalAggregateSort.sort(result, sortOptions);
			writeToJson("After sort" ,result);
			Assert.assertEquals(null, result.getChildren());
			Assert.assertEquals(2, result.getSortedList().size(), 2);
			Iterator<Map.Entry<String, HierarchicalAggregateEntity>> it = result.getSortedList().iterator();
			Assert.assertEquals(true, it.hasNext());
			Map.Entry<String, HierarchicalAggregateEntity> entry = it.next();
			Assert.assertEquals("cluster1", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(1), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+entities[2].getNumClusters()));
			
			Assert.assertEquals(true, it.hasNext());
			entry = it.next();
			Assert.assertEquals("cluster2", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(1), (double)(entities[3].getNumClusters()+entities[4].getNumClusters()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	
	@Test
	public void testMultipleGropubyFieldsHierarchicalAggregator(){ 
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc2", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		HierarchicalAggregator agg = new HierarchicalAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate", result);
			Assert.assertEquals(2, result.getChildren().size());
			Assert.assertEquals(66.0, (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(2, result.getChildren().get("cluster1").getChildren().size());
			Assert.assertEquals(result.getChildren().get("cluster1").getChildren().get("dc1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster1").getChildren().get("dc2").getValues().get(0), (double)(entities[2].getNumHosts()));
			
			Assert.assertEquals(result.getChildren().get("cluster2").getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(1, result.getChildren().get("cluster2").getChildren().size());
			Assert.assertEquals(result.getChildren().get("cluster2").getChildren().get("dc1").getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			
			// test sort by function 2
			SortOption so = new SortOption();
			so.setIndex(0);
			so.setAscendant(true);
			List<SortOption> sortOptions = Arrays.asList(so);
			PostHierarchicalAggregateSort.sort(result, sortOptions);
			writeToJson("After sort" ,result);
			Assert.assertEquals(null, result.getChildren());
			Assert.assertEquals(2, result.getSortedList().size());
			Iterator<Map.Entry<String, HierarchicalAggregateEntity>> it = result.getSortedList().iterator();
			Assert.assertEquals(true, it.hasNext());
			Map.Entry<String, HierarchicalAggregateEntity> entry = it.next();
			Assert.assertEquals("cluster2", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			
			Assert.assertEquals(true, it.hasNext());
			entry = it.next();
			Assert.assertEquals("cluster1", entry.getKey());
			Assert.assertEquals(entry.getValue().getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));			
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testUnassigned(){ 
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntityWithoutDatacenter("cluster1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntityWithoutDatacenter("cluster2", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		HierarchicalAggregator agg = new HierarchicalAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate", result);
			Assert.assertEquals(result.getChildren().size(), 2);
			Assert.assertEquals(result.getChildren().get("dc1").getValues().get(0), (double)(entities[1].getNumHosts()+entities[2].getNumHosts())+entities[4].getNumHosts());
			Assert.assertEquals(result.getChildren().get("unassigned").getValues().get(0), (double)(entities[0].getNumHosts()+entities[3].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new HierarchicalAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			HierarchicalAggregateEntity result = agg.result();
			writeToJson("After aggregate", result);
			Assert.assertEquals(result.getChildren().size(), 2);
			Assert.assertEquals(result.getChildren().get("cluster1").getValues().get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(2, result.getChildren().get("cluster1").getChildren().size());
			Assert.assertEquals(result.getChildren().get("cluster1").getChildren().get("dc1").getValues().get(0), (double)(entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster1").getChildren().get("unassigned").getValues().get(0), (double)(entities[0].getNumHosts()));
			
			Assert.assertEquals(result.getChildren().get("cluster2").getValues().get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster2").getChildren().get("dc1").getValues().get(0), (double)(entities[4].getNumHosts()));
			Assert.assertEquals(result.getChildren().get("cluster2").getChildren().get("unassigned").getValues().get(0), (double)(entities[3].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
}
