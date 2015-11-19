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

import junit.framework.Assert;

import org.apache.eagle.query.aggregate.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.query.aggregate.Aggregator;

public class TestAggregator {
	private final static Logger LOG = LoggerFactory.getLogger(TestAggregator.class);
	
	public static class AggregatedSampleAPIEntityFactory implements AggregateAPIEntityFactory {
		@Override
		public AggregateAPIEntity create(){
			return new AggregatedSampleAPIEntity();
		}
	}
	
	
	public static class TestAPIEntity extends TaggedLogAPIEntity{
		private String numTotalAlerts;
		private String usedCapacity;
		private String status;

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public String getNumTotalAlerts() {
			return numTotalAlerts;
		}

		public void setNumTotalAlerts(String numTotalAlerts) {
			this.numTotalAlerts = numTotalAlerts;
		}

		public String getUsedCapacity() {
			return usedCapacity;
		}

		public void setUsedCapacity(String usedCapacity) {
			this.usedCapacity = usedCapacity;
		}
	}
	


	public static class AggregatedSampleAPIEntity extends AggregateAPIEntity{
		private long numTotalAlerts;
	
		@JsonProperty("nTA")
		public long getNumTotalAlerts() {
			return numTotalAlerts;
		}
	
		public void setNumTotalAlerts(long numTotalAlerts) {
			this.numTotalAlerts = numTotalAlerts;
		}
	}
	
	@Test
	public void testAggregate(){ 
		try{
			final AggregatedSampleAPIEntity root = new AggregatedSampleAPIEntity();
			List<String> sumFunctionFields = Arrays.asList("numTotalAlerts");
			boolean counting = true;
			List<String> groupbys = Arrays.asList(Aggregator.GROUPBY_ROOT_FIELD_NAME, "cluster");
			List<AggregateParams.SortFieldOrder> sortFieldOrders = new ArrayList<AggregateParams.SortFieldOrder>();
			sortFieldOrders.add(new AggregateParams.SortFieldOrder("numTotalAlerts", false));
			Aggregator agg = new Aggregator(new AggregatedSampleAPIEntityFactory(), root, groupbys, counting, sumFunctionFields);
			List<TestAPIEntity> list = new ArrayList<TestAPIEntity>();
			TestAPIEntity entity = new TestAPIEntity();
			entity.setTags(new HashMap<String, String>());
			entity.getTags().put("category", "checkHadoopFS");
			entity.getTags().put("rack", "rack123");
			entity.getTags().put("cluster", "cluster1");
			entity.setNumTotalAlerts("123");
			entity.setUsedCapacity("12.5");
			entity.setStatus("live");
			list.add(entity);
	
			TestAPIEntity entity2 = new TestAPIEntity();
			entity2.setTags(new HashMap<String, String>());
			entity2.getTags().put("category", "checkHadoopFS");
			entity2.getTags().put("rack", "rack124");
			entity2.getTags().put("cluster", "cluster2");
			entity2.setNumTotalAlerts("35");
			entity2.setUsedCapacity("32.1");
			entity2.setStatus("dead");
			list.add(entity2);
			
			TestAPIEntity entity3 = new TestAPIEntity();
			entity3.setTags(new HashMap<String, String>());
			entity3.getTags().put("category", "checkHadoopFS");
	//		entity3.getTags().put("rack", "rack124");
			entity3.getTags().put("cluster", "cluster2");
			entity3.setNumTotalAlerts("11");
			entity3.setUsedCapacity("82.11");
			entity3.setStatus("live");
			list.add(entity3);
						
			TestAPIEntity entity4 = new TestAPIEntity();
			entity4.setTags(new HashMap<String, String>());
			entity4.getTags().put("category", "diskfailure");
			entity4.getTags().put("rack", "rack124");
			entity4.getTags().put("cluster", "cluster2");
			entity4.setNumTotalAlerts("61");
			entity4.setUsedCapacity("253.2");
			entity4.setStatus("dead");
			list.add(entity4);
			
			long numTotalAlerts = 0;
			for(TestAPIEntity e : list){
				agg.accumulate(e);
				numTotalAlerts += Long.valueOf(e.getNumTotalAlerts());
			}

			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			String result = null;
			AggregatedSampleAPIEntity toBeVerified = (AggregatedSampleAPIEntity)root.getEntityList().get(Aggregator.GROUPBY_ROOT_FIELD_VALUE);
			result = mapper.writeValueAsString(toBeVerified);
			
			Assert.assertEquals(2, toBeVerified.getNumDirectDescendants());
			Assert.assertEquals(4, toBeVerified.getNumTotalDescendants());
			Assert.assertEquals(numTotalAlerts, toBeVerified.getNumTotalAlerts());
			
	    	LOG.info(result);
	    	
	    	PostAggregateSorting.sort(root, sortFieldOrders);
	    	toBeVerified = (AggregatedSampleAPIEntity)root.getSortedList().get(0);
			result = mapper.writeValueAsString(toBeVerified);
	    	LOG.info(result);
	    }catch(Exception ex){
	    	LOG.error("Test aggregator fails", ex);
	    	Assert.fail("Test aggregator fails");
	    }
	}
	
	@Test
	public void testUnassigned(){ 
		// rack is unassigned
		try{
			final AggregatedSampleAPIEntity root = new AggregatedSampleAPIEntity();
			boolean counting = true;
			List<String> groupbys = Arrays.asList(Aggregator.GROUPBY_ROOT_FIELD_NAME, "rack");
			List<AggregateParams.SortFieldOrder> sortFieldOrders = new ArrayList<AggregateParams.SortFieldOrder>();
			sortFieldOrders.add(new AggregateParams.SortFieldOrder("count", false));
			sortFieldOrders.add(new AggregateParams.SortFieldOrder("key", false));
			Aggregator agg = new Aggregator(new AggregatedSampleAPIEntityFactory(), root, groupbys, counting, new ArrayList<String>());
			List<TestAPIEntity> list = new ArrayList<TestAPIEntity>();
			TestAPIEntity entity = new TestAPIEntity();
			entity.setTags(new HashMap<String, String>());
			entity.getTags().put("category", "checkHadoopFS");
			entity.getTags().put("rack", "rack123");
			entity.getTags().put("cluster", "cluster1");
			entity.setNumTotalAlerts("123");
			entity.setUsedCapacity("12.5");
			entity.setStatus("live");
			list.add(entity);
	
			TestAPIEntity entity2 = new TestAPIEntity();
			entity2.setTags(new HashMap<String, String>());
			entity2.getTags().put("category", "checkHadoopFS");
			entity2.getTags().put("rack", "rack124");
			entity2.getTags().put("cluster", "cluster2");
			entity2.setNumTotalAlerts("35");
			entity2.setUsedCapacity("32.1");
			entity2.setStatus("dead");
			list.add(entity2);
			
			TestAPIEntity entity3 = new TestAPIEntity();
			entity3.setTags(new HashMap<String, String>());
			entity3.getTags().put("category", "checkHadoopFS");
	//		entity3.getTags().put("rack", "rack124");
			entity3.getTags().put("cluster", "cluster2");
			entity3.setNumTotalAlerts("11");
			entity3.setUsedCapacity("82.11");
			entity3.setStatus("live");
			list.add(entity3);
						
			TestAPIEntity entity4 = new TestAPIEntity();
			entity4.setTags(new HashMap<String, String>());
			entity4.getTags().put("category", "diskfailure");
			entity4.getTags().put("rack", "rack124");
			entity4.getTags().put("cluster", "cluster2");
			entity4.setNumTotalAlerts("61");
			entity4.setUsedCapacity("253.2");
			entity4.setStatus("dead");
			list.add(entity4);
			
//			long numTotalAlerts = 0;
			for(TestAPIEntity e : list){
				agg.accumulate(e);
//				numTotalAlerts += Long.valueOf(e.getNumTotalAlerts());
			}

			JsonFactory factory = new JsonFactory(); 
			ObjectMapper mapper = new ObjectMapper(factory);
			String result = null;
			AggregatedSampleAPIEntity toBeVerified = (AggregatedSampleAPIEntity)root.getEntityList().get(Aggregator.GROUPBY_ROOT_FIELD_VALUE);
			result = mapper.writeValueAsString(toBeVerified);
			
			Assert.assertEquals(3, toBeVerified.getNumDirectDescendants());
			Assert.assertEquals(4, toBeVerified.getNumTotalDescendants());
//			Assert.assertEquals(numTotalAlerts, toBeVerified.getNumTotalAlerts());
			
	    	LOG.info(result);
	    	
	    	PostAggregateSorting.sort(root, sortFieldOrders);			
	    	toBeVerified = (AggregatedSampleAPIEntity)root.getSortedList().get(0);
			result = mapper.writeValueAsString(toBeVerified);
	    	LOG.info(result);
	    }catch(Exception ex){
	    	LOG.error("Test aggregator fails", ex);
	    	Assert.fail("Test aggregator fails");
	    }
	}
}
