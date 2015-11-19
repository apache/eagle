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
package org.apache.eagle.storage.hbase.aggregate.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateClientImpl;
import junit.framework.Assert;

import org.apache.eagle.storage.hbase.query.coprocessor.AggregateClient;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.apache.eagle.service.hbase.TestHBaseBase;

/**
 * @since : 10/30/14,2014
 */
public class TestGroupAggregateClient extends TestHBaseBase {
	HTableInterface table;
	long startTime;
	long endTime;
	List<String> rowkeys;
	AggregateClient client;
	Scan scan;
	int num = 200;

	private final static Logger LOG = LoggerFactory.getLogger(TestGroupAggregateClient.class);

	@Before
	public void setUp(){
		hbase.createTable("unittest", "f");
		startTime = System.currentTimeMillis();
		try {
			rowkeys = prepareData(num);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		endTime = System.currentTimeMillis();
		table = EagleConfigFactory.load().getHTable("unittest");
		client = new AggregateClientImpl();
		scan = new Scan();
		scan.setCaching(200);
		
		ListQueryCompiler compiler = null;
		try {
			compiler = new ListQueryCompiler("TestLogAPIEntity[@cluster=\"test4UT\" and @datacenter=\"dc1\"]{@field1,@field2}");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
		scan.setFilter(compiler.filter());
	}
	
	@After
	public void shutdown(){
		try {
			hbase.deleteTable("unittest");
			new HTableFactory().releaseHTableInterface(table);
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}
	}
	
	private List<String> prepareData(int count) throws Exception {
		List<TaggedLogAPIEntity> list = new ArrayList<TaggedLogAPIEntity>();
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);

		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}
		ed.setTimeSeries(true);
		for(int i=0;i<count;i++){
			TestLogAPIEntity e = new TestLogAPIEntity();
			e.setTimestamp(System.currentTimeMillis());
			e.setField1(1);
			e.setField2(2);
			e.setField3(3);
			e.setField4(4L);
			e.setField5(5.0);
			e.setField6(5.0);
			e.setField7("7");
			e.setTags(new HashMap<String, String>());
			e.getTags().put("cluster", "test4UT");
			e.getTags().put("datacenter", "dc1");
			e.getTags().put("index", ""+i);
			e.getTags().put("jobId", "job_"+System.currentTimeMillis());
			list.add(e);

		}
		GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
		LOG.info("Writing "+list.size()+" TestLogAPIEntity entities");
		List<String> result = writer.write(list);
		LOG.info("Finish writing test entities");
		return result;
	}

	//@Test
	public void testGroupAggregateCountClient(){
		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.count),Arrays.asList("field2")).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("COUNT");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size()>0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupAggregateAvgClient(){
		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.avg),Arrays.asList("field2")).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("AVG");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size()>0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupAggregateMaxClient(){
		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.max),Arrays.asList("field1")).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MAX");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size()>0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupAggregateSumClient(){
		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.sum),Arrays.asList("field2")).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MAX");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size()>0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupAggregateMinClient(){

		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.min),Arrays.asList("field2")).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MIN");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size()>0);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupAggregateMultipleClient(){
		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),
					Arrays.asList(AggregateFunctionType.min,
							AggregateFunctionType.max,
							AggregateFunctionType.avg,
							AggregateFunctionType.count,
							AggregateFunctionType.sum),
					Arrays.asList("field2","field2","field2","field2","field2")).getKeyValues();
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
			Assert.assertTrue(result.size() > 0);
			Assert.assertEquals("test4UT", new String(result.get(0).getKey().getValue().get(0).copyBytes()));
			Assert.assertEquals("dc1", new String(result.get(0).getKey().getValue().get(1).copyBytes()));
			Assert.assertEquals(2.0, result.get(0).getValue().get(0).get());
			Assert.assertEquals(2.0, result.get(0).getValue().get(1).get());
			Assert.assertEquals(2.0, result.get(0).getValue().get(2).get());
			Assert.assertTrue(num <= result.get(0).getValue().get(3).get());
			Assert.assertTrue(2.0 * num <= result.get(0).getValue().get(4).get());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private void logGroupbyKeyValue(List<GroupbyKeyValue> keyValues){
		for(GroupbyKeyValue keyValue:keyValues){
			GroupbyKey key = keyValue.getKey();
			List<String> keys = new ArrayList<String>();
			for(BytesWritable bytes:key.getValue()){
				keys.add(new String(bytes.copyBytes()));
			}
			List<Double> vals = new ArrayList<Double>();
			GroupbyValue val = keyValue.getValue();
			for(DoubleWritable dw:val.getValue()){
				vals.add(dw.get());
			}
			if(LOG.isDebugEnabled()) LOG.debug("KEY: "+keys+", VALUE: "+vals);
		}
	}
}