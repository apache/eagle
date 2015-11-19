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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.eagle.query.aggregate.timeseries.PostFlatAggregateSort;
import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import org.apache.eagle.query.aggregate.timeseries.SortOption;

public class TestPostFlatAggregateSort {
	private static final Logger logger = Logger.getLogger(TestPostFlatAggregateSort.class);
	@Test
	public void testSort(){
		final String aggField1Value1 = "field1value1";
		final String aggField1Value2 = "field1value2";
		final String aggField2Value1 = "field2value1";
		final String aggField2Value2 = "field2value2";
		final Double d1 = new Double(1);
		final Double d2 = new Double(2);
		final Double d3 = new Double(3);
		final Double d4 = new Double(4);
		@SuppressWarnings("serial")
		Map<List<String>, List<Double>> result = new HashMap<List<String>, List<Double>>(){{
			put(Arrays.asList(aggField1Value1, aggField2Value1), Arrays.asList(d2, d3));
			put(Arrays.asList(aggField1Value2, aggField2Value2), Arrays.asList(d1, d4));
		}};
		
		// sort by function1
		SortOption so = new SortOption();
		so.setIndex(0);
		so.setAscendant(true);
		List<SortOption> sortOptions = Arrays.asList(so);
		List<Map.Entry<List<String>, List<Double>>> set = 
				PostFlatAggregateSort.sort(result, sortOptions, 0);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		Assert.assertEquals(2, set.size());
		Iterator<Map.Entry<List<String>, List<Double>>> it = set.iterator();
		Map.Entry<List<String>, List<Double>> e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value2));
		Assert.assertTrue(e.getValue().get(0).equals(d1));
		e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value1));
		Assert.assertTrue(e.getValue().get(0).equals(d2));
		try{
			String value = mapper.writeValueAsString(set);
			logger.info(value);
		}catch(Exception ex){
			logger.error("fail with mapping", ex);
			Assert.fail("fail with mapping");
		}
		
		
		// sort by function2
		so = new SortOption();
		so.setIndex(1);
		so.setAscendant(true);
		sortOptions = Arrays.asList(so);
		set = PostFlatAggregateSort.sort(result, sortOptions, 0);
		factory = new JsonFactory();
		mapper = new ObjectMapper(factory);
		Assert.assertEquals(2, set.size());
		it = set.iterator();
		e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value1));
		Assert.assertTrue(e.getValue().get(0).equals(d2));
		e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value2));
		Assert.assertTrue(e.getValue().get(0).equals(d1));
		try{
			String value = mapper.writeValueAsString(set);
			logger.info(value);
		}catch(Exception ex){
			logger.error("fail with mapping", ex);
			Assert.fail("fail with mapping");
		}
	}
	
	@Test
	public void testDefaultSort(){
		final String aggField1Value1 = "xyz";
		final String aggField1Value2 = "xyz";
		final String aggField2Value1 = "abd";
		final String aggField2Value2 = "abc";
		final Double d1 = new Double(1);
		final Double d2 = new Double(1);
		@SuppressWarnings("serial")
		Map<List<String>, List<Double>> result = new HashMap<List<String>, List<Double>>(){{
			put(Arrays.asList(aggField1Value1, aggField2Value1), Arrays.asList(d2));
			put(Arrays.asList(aggField1Value2, aggField2Value2), Arrays.asList(d1));
		}};
		
		// sort by function1
		SortOption so = new SortOption();
		so.setIndex(0);
		so.setAscendant(true);
		List<SortOption> sortOptions = Arrays.asList(so);
		List<Map.Entry<List<String>, List<Double>>> set = 
				PostFlatAggregateSort.sort(result, sortOptions, 0);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		Assert.assertEquals(2, set.size());
		Iterator<Map.Entry<List<String>, List<Double>>> it = set.iterator();
		Map.Entry<List<String>, List<Double>> e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value2));
		Assert.assertTrue(e.getValue().get(0).equals(d1));
		e = it.next();
		Assert.assertTrue(e.getKey().get(0).equals(aggField1Value1));
		Assert.assertTrue(e.getValue().get(0).equals(d2));
		try{
			String value = mapper.writeValueAsString(set);
			logger.info(value);
		}catch(Exception ex){
			logger.error("fail with mapping", ex);
			Assert.fail("fail with mapping");
		}
	}
}
