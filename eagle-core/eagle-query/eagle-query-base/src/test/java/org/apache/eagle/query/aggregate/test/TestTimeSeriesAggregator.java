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

import org.apache.eagle.query.aggregate.timeseries.TimeSeriesAggregator;
import junit.framework.Assert;

import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.test.TestEntity;

public class TestTimeSeriesAggregator {
	private static final Logger LOG = LoggerFactory.getLogger(TestFlatAggregator.class);
	@SuppressWarnings("serial")
	private TestEntity createEntity(final String cluster, final String datacenter, final String rack, int numHosts, long numClusters, long timestamp){
		TestEntity entity = new TestEntity();
		Map<String, String> tags = new HashMap<String, String>(){{
			put("cluster", cluster);
			put("datacenter", datacenter);
			put("rack", rack);
		}}; 
		entity.setTags(tags);
		entity.setNumHosts(numHosts);
		entity.setNumClusters(numClusters);
		entity.setTimestamp(timestamp);
		return entity;
	}
	
	@Test
	public void testTimeSeriesAggregator(){
		TestEntity[] entities = new TestEntity[8];
		entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2, 1386120000*1000); // bucket 0
		entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1, 1386121060*1000); // bucket 17
		entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0, 1386121070*1000); // bucket 17
		entities[3] = createEntity("cluster2", "dc1", "rack125", 9,   2, 1386122122*1000); // bucket 35
		entities[4] = createEntity("cluster2", "dc1", "rack126", 15,  5, 1386123210*1000); // bucket 53
		entities[5] = createEntity("cluster2", "dc1", "rack234", 25,  1, 1386123480*1000); // bucket 58
		entities[6] = createEntity("cluster2", "dc1", "rack234", 12,  0, 1386123481*1000); // bucket 58
		entities[7] = createEntity("cluster1", "dc1", "rack123", 3,    2, 1386123482*1000); // bucket 58
		
		TimeSeriesAggregator tsAgg = new TimeSeriesAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"),
				1386120000*1000, 1386123600*1000, 60*1000);
		try{
			for(TestEntity e : entities){
				tsAgg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = tsAgg.result();
			Assert.assertEquals(result.size(), 6);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "0")).get(0), (double)(entities[0].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "17")).get(0), (double)(entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "35")).get(0), (double)(entities[3].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "53")).get(0), (double)(entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "58")).get(0), (double)(entities[5].getNumHosts()+entities[6].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "58")).get(0), (double)(entities[7].getNumHosts()));
			
			Map<List<String>, List<double[]>> tsResult = tsAgg.getMetric();
			Assert.assertEquals(tsResult.size(), 2);
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0).length, 60);
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[0], (double)(entities[0].getNumHosts()));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[17], (double)(entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[35], (double)(entities[3].getNumHosts()));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[53], (double)(entities[4].getNumHosts()));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[58], (double)(entities[5].getNumHosts()+entities[6].getNumHosts()));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[58], (double)(entities[7].getNumHosts()));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		tsAgg = new TimeSeriesAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), 
				1386120000*1000, 1386123600*1000, 60*1000);
		try{
			for(TestEntity e : entities){
				tsAgg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = tsAgg.result();
			Assert.assertEquals(result.size(), 5);
			Assert.assertEquals(result.get(Arrays.asList("0")).get(0), (double)(entities[0].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("17")).get(0), (double)(entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("35")).get(0), (double)(entities[3].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("53")).get(0), (double)(entities[4].getNumHosts()));
			Assert.assertEquals(result.get(Arrays.asList("58")).get(0), (double)(entities[5].getNumHosts()+entities[6].getNumHosts()+entities[7].getNumHosts()));
			
			Map<List<String>, List<double[]>> tsResult = tsAgg.getMetric();
			Assert.assertEquals(tsResult.size(), 1);
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0).length, 60);
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[0], (double)(entities[0].getNumHosts()));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[17], (double)(entities[1].getNumHosts()+entities[2].getNumHosts()));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[35], (double)(entities[3].getNumHosts()));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[53], (double)(entities[4].getNumHosts()));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[58], (double)(entities[5].getNumHosts()+entities[6].getNumHosts()+entities[7].getNumHosts()));		
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		tsAgg = new TimeSeriesAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), 
				1386120000*1000, 1386123600*1000, 60*1000);
		try{
			for(TestEntity e : entities){
				tsAgg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = tsAgg.result();
			Assert.assertEquals(result.size(), 6);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "0")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "17")).get(0), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "35")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "53")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "58")).get(0), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "58")).get(0), (double)(1));
			
			Map<List<String>, List<double[]>> tsResult = tsAgg.getMetric();
			Assert.assertEquals(tsResult.size(), 2);
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0).length, 60);
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[0], (double)(1));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[17], (double)(2));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[35], (double)(1));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[53], (double)(1));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster2")).get(0)[58], (double)(2));
			Assert.assertEquals(tsResult.get(Arrays.asList("cluster1")).get(0)[58], (double)(1));
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		tsAgg = new TimeSeriesAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), 
				1386120000*1000, 1386123600*1000, 60*1000);
		try{
			for(TestEntity e : entities){
				tsAgg.accumulate(e);
			}
			Map<List<String>, List<Double>> result = tsAgg.result();
			Assert.assertEquals(result.size(), 5);
			Assert.assertEquals(result.get(Arrays.asList("0")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("17")).get(0), (double)(2));
			Assert.assertEquals(result.get(Arrays.asList("35")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("53")).get(0), (double)(1));
			Assert.assertEquals(result.get(Arrays.asList("58")).get(0), (double)(3));
			
			Map<List<String>, List<double[]>> tsResult = tsAgg.getMetric();
			Assert.assertEquals(tsResult.size(), 1);
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0).length, 60);
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[0], (double)(1));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[17], (double)(2));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[35], (double)(1));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[53], (double)(1));
			Assert.assertEquals(tsResult.get(new ArrayList<String>()).get(0)[58], (double)(3));		
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
}
