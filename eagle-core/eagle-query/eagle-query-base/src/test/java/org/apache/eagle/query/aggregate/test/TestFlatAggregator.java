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
import java.util.List;
import java.util.Map;

import org.apache.eagle.query.aggregate.timeseries.FlatAggregator;
import junit.framework.Assert;

import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.test.TestEntity;

public class TestFlatAggregator {
	private static final Logger LOG = LoggerFactory.getLogger(TestFlatAggregator.class);
	@Test
	public void testCounting(){
		
	}
	
	@Test
	public void testSummary(){
		
	}
	
	@Test
	public void testAverage(){
		
	}
	
	@Test
	public void testIterativeAggregation(){
		
	}
	
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

	@Test
	public void testZeroGroupbyFieldSingleFunctionForSummary(){
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		
		FlatAggregator agg = new FlatAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+
					entities[2].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+
					entities[2].getNumClusters()+entities[3].getNumClusters()+entities[4].getNumClusters()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0), (double)(5));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testSingleGroupbyFieldSingleFunctionForSummary(){
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc2", "rack126", 15, 2);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts())+entities[3].getNumHosts());
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), (double)(entities[4].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+entities[2].getNumClusters()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), (double)(entities[3].getNumClusters()+entities[4].getNumClusters()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+entities[2].getNumClusters())+entities[3].getNumClusters());
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), (double)(entities[4].getNumClusters()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	
	@Test
	public void testSingleGroupbyFieldSingleFunctionForCount(){
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc2", "rack126", 15, 2);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), (double)(3));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), (double)(2));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(4));
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), (double)(1));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testMultipleFieldsSingleFunctionForSummary(){
		TestEntity[] entities = new TestEntity[6];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		entities[5] = createEntity("cluster2", null, "rack126", 1, 3);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(3, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1")).get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned")).get(0), (double)(entities[5].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("cluster", "datacenter", "rack"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(5, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack123")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack128")).get(0), (double)(entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack125")).get(0), (double)(entities[3].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack126")).get(0), (double)(entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned", "rack126")).get(0), (double)(entities[5].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testMultipleFieldsSingleFunctionForCount(){
		TestEntity[] entities = new TestEntity[6];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		entities[5] = createEntity("cluster2", null, "rack126", 1, 3);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(3, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1")).get(0), (double)(3));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1")).get(0), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned")).get(0), (double)(1));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("cluster", "datacenter", "rack"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(5, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack123")).get(0), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack128")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack125")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack126")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned", "rack126")).get(0), (double)(1));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testSingleGroupbyFieldMultipleFunctions(){
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum, AggregateFunctionType.count), 
				Arrays.asList("numHosts", "*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(1), (double)(3));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), (double)(entities[3].getNumHosts()+entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(1), (double)(2));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count, AggregateFunctionType.sum), Arrays.asList("*", "numHosts"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(5));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(1), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()+entities[3].getNumHosts())+entities[4].getNumHosts());
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new FlatAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count, AggregateFunctionType.sum, AggregateFunctionType.sum), 
				Arrays.asList("*", "numHosts", "numClusters"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(5));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(1), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()+entities[2].getNumHosts()+entities[3].getNumHosts())+entities[4].getNumHosts());
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(2), (double)(entities[0].getNumClusters()+entities[1].getNumClusters()+entities[2].getNumClusters()+entities[3].getNumClusters())+entities[4].getNumClusters());
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testMultipleGroupbyFieldsMultipleFunctions(){
		TestEntity[] entities = new TestEntity[5];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15, 2);
		
		FlatAggregator agg = new FlatAggregator(Arrays.asList("cluster", "rack"), Arrays.asList(AggregateFunctionType.sum, AggregateFunctionType.count), 
				Arrays.asList("numHosts", "*"));
		try{
			for(TestEntity e : entities){
				agg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 4);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack123")).get(0), (double)(entities[0].getNumHosts()+entities[1].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack123")).get(1), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack128")).get(0), (double)(entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack128")).get(1), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack125")).get(0), (double)(entities[3].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack125")).get(1), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack126")).get(0), (double)(entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack126")).get(1), (double)(1));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
}
