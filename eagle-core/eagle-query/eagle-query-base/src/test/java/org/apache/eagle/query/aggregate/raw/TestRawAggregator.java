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
package org.apache.eagle.query.aggregate.raw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.log.entity.meta.IntSerDeser;
import org.apache.eagle.log.entity.meta.LongSerDeser;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.common.ByteUtil;

public class TestRawAggregator {
	private static final Logger LOG = LoggerFactory.getLogger(TestRawAggregator.class);
	
	private EntityDefinition ed;
	@SuppressWarnings("unchecked")
	@Before
	public void setup(){
		ed = new EntityDefinition();
		Qualifier q = new Qualifier();
		q.setDisplayName("numHosts");
		q.setQualifierName("a");
		EntitySerDeser<?> serDeser = new IntSerDeser();
		q.setSerDeser((EntitySerDeser<Object>)(serDeser));
		ed.getDisplayNameMap().put("numHosts", q);
		q = new Qualifier();
		q.setDisplayName("numClusters");
		q.setQualifierName("b");
		serDeser = new LongSerDeser();
		q.setSerDeser((EntitySerDeser<Object>)(serDeser));
		ed.getDisplayNameMap().put("numClusters", q);
	}
	
	private Map<String, byte[]> createQualifiers(final String cluster, final String datacenter, final String rack, int numHosts, long numClusters){
		Map<String, byte[]> qualifiers = new HashMap<String, byte[]>();
		qualifiers.put("cluster", cluster == null ? null : cluster.getBytes());
		qualifiers.put("datacenter", datacenter == null ? null : datacenter.getBytes());
		qualifiers.put("rack", rack == null ? null : rack.getBytes());
		qualifiers.put("numHosts", ByteUtil.intToBytes(numHosts));
		qualifiers.put("numClusters", ByteUtil.longToBytes(numClusters));
		return qualifiers;
	}

	@Test
	public void testZeroGroupbyFieldSingleFunctionForSummary(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc1", "rack126", 15, 2));
		
		RawAggregator agg = new RawAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			
			double total = 0.0;
			for(Map<String, byte[]> e : entities){
				int a = ByteUtil.bytesToInt(e.get("numHosts"));
				total += a;
			}
			
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0).doubleValue(), total, 0.00000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			double total = 0.0;
			for(Map<String, byte[]> e : entities){
				long a = ByteUtil.bytesToLong(e.get("numClusters"));
				total += a;
			}
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0).doubleValue(), total, 0.00000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(new ArrayList<String>(), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 1);
			Assert.assertEquals(result.get(new ArrayList<String>()).get(0).doubleValue(), 5, 0.0000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testSingleGroupbyFieldSingleFunctionForSummary(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc2", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc1", "rack126", 15, 2));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total1 = 0.0;
			total1 += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			
			double total2 = 0.0;
			total2 += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total2 += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0).doubleValue(), total1, 0.0000000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), total2, 0.00000000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total1 = 0.0;
			total1 += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			
			double total2 = 0.0;
			total2 += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), total1, 0.000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), total2, 0.000000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total1 = 0.0;
			total1 += ByteUtil.bytesToLong(entities.get(0).get("numClusters"));
			total1 += ByteUtil.bytesToLong(entities.get(1).get("numClusters"));
			total1 += ByteUtil.bytesToLong(entities.get(2).get("numClusters"));
			
			double total2 = 0.0;
			total2 += ByteUtil.bytesToLong(entities.get(3).get("numClusters"));
			total2 += ByteUtil.bytesToLong(entities.get(4).get("numClusters"));
			
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), total1, 0.0000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), total2, 0.0000000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numClusters"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total1 = 0.0;
			total1 += ByteUtil.bytesToLong(entities.get(0).get("numClusters"));
			total1 += ByteUtil.bytesToLong(entities.get(1).get("numClusters"));
			total1 += ByteUtil.bytesToLong(entities.get(3).get("numClusters"));
			total1 += ByteUtil.bytesToLong(entities.get(4).get("numClusters"));
			
			double total2 = 0.0;
			total2 += ByteUtil.bytesToLong(entities.get(2).get("numClusters"));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), total1, 0.00000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), total2, 0.00000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	
	@Test
	public void testSingleGroupbyFieldSingleFunctionForCount(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc2", "rack126", 15, 2));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total1 = 0.0;
			total1 += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total1 += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			
			double total2 = 0.0;
			total2 += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total2 += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), total1, 0.0000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), total2, 0.0000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(4), 0.00000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(0), (double)(1), 0.00000000000000000000001);
		}catch(Exception ex){
			LOG.error("can not aggregate", ex);
			Assert.fail("can not aggregate");
		}
	}
	
	@Test
	public void testMultipleFieldsSingleFunctionForSummary(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc1", "rack126", 15, 2));
		entities.add(createQualifiers("cluster2", null, "rack126", 1, 3));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(3, result.size());
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1")).get(0), total, 0.00000000000000000000000001);
			
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1")).get(0), total, 0.0000000000000000000000001);
			
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1")).get(0), total, 0.0000000000000000000000001);
			
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(5).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned")).get(0), total, 0.0000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("cluster", "datacenter", "rack"), Arrays.asList(AggregateFunctionType.sum), Arrays.asList("numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(5, result.size());
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack123")).get(0), total, 0.0000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack128")).get(0), total, 0.0000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack125")).get(0), total, 0.0000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack126")).get(0), total, 0.0000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(5).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned", "rack126")).get(0), total, 0.0000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testMultipleFieldsSingleFunctionForCount(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc1", "rack126", 15, 2));
		entities.add(createQualifiers("cluster2", null, "rack126", 1, 3));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster", "datacenter"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(3, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1")).get(0), (double)(3), 0.00000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1")).get(0), (double)(2), 0.0000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned")).get(0), (double)(1), 0.000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("cluster", "datacenter", "rack"), Arrays.asList(AggregateFunctionType.count), Arrays.asList("*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(5, result.size());
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack123")).get(0), (double)(2), 0.0000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "dc1", "rack128")).get(0), (double)(1), 0.0000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack125")).get(0), (double)(1), 0.0000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "dc1", "rack126")).get(0), (double)(1), 0.0000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "unassigned", "rack126")).get(0), (double)(1), 0.0000000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testSingleGroupbyFieldMultipleFunctions(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc2", "rack126", 15, 2));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster"), Arrays.asList(AggregateFunctionType.sum, AggregateFunctionType.count), 
				Arrays.asList("numHosts", "*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(0), total, 0.0000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster1")).get(1), (double)(3), 0.00000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(0), total, 0.0000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2")).get(1), (double)(2), 0.0000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count, AggregateFunctionType.sum), Arrays.asList("*", "numHosts"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(4), 0.00000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(1), total, 0.00000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
		
		agg = new RawAggregator(Arrays.asList("datacenter"), Arrays.asList(AggregateFunctionType.count, AggregateFunctionType.sum, AggregateFunctionType.sum), 
				Arrays.asList("*", "numHosts", "numClusters"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 2);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(4), 0.000000000000000000000000001);
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(1), total, 0.0000000000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToLong(entities.get(0).get("numClusters"));
			total += ByteUtil.bytesToLong(entities.get(1).get("numClusters"));
			total += ByteUtil.bytesToLong(entities.get(2).get("numClusters"));
			total += ByteUtil.bytesToLong(entities.get(3).get("numClusters"));
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(2), total, 0.00000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("dc1")).get(0), (double)(4), 0.000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(1), total, 0.00000000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToLong(entities.get(4).get("numClusters"));
			Assert.assertEquals(result.get(Arrays.asList("dc2")).get(2), total, 0.000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
	
	@Test
	public void testMultipleGroupbyFieldsMultipleFunctions(){
		List<Map<String, byte[]>> entities = new ArrayList<Map<String, byte[]>>();
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 12, 2));
		entities.add(createQualifiers("cluster1", "dc1", "rack123", 20, 1));
		entities.add(createQualifiers("cluster1", "dc1", "rack128", 10, 0));
		entities.add(createQualifiers("cluster2", "dc1", "rack125", 9, 2));
		entities.add(createQualifiers("cluster2", "dc1", "rack126", 15, 2));
		
		RawAggregator agg = new RawAggregator(Arrays.asList("cluster", "rack"), Arrays.asList(AggregateFunctionType.sum, AggregateFunctionType.count), 
				Arrays.asList("numHosts", "*"), ed);
		try{
			for(Map<String, byte[]> e : entities){
				agg.qualifierCreated(e);
			}
			Map<List<String>, List<Double>> result = agg.result();
			Assert.assertEquals(result.size(), 4);
			double total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(0).get("numHosts"));
			total += ByteUtil.bytesToInt(entities.get(1).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack123")).get(0), total, 0.000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack123")).get(1), (double)(2), 0.00000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(2).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack128")).get(0), total, 0.00000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster1", "rack128")).get(1), (double)(1), 0.00000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(3).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack125")).get(0), total, 0.000000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack125")).get(1), (double)(1), 0.0000000000000000000000001);
			total = 0.0;
			total += ByteUtil.bytesToInt(entities.get(4).get("numHosts"));
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack126")).get(0), total, 0.00000000000000000000000001);
			Assert.assertEquals(result.get(Arrays.asList("cluster2", "rack126")).get(1), (double)(1), 0.000000000000000000000000001);
		}catch(Exception ex){
			LOG.error("Can not aggregate", ex);
			Assert.fail("Can not aggregate");
		}
	}
}
