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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import junit.framework.Assert;

import org.apache.eagle.storage.hbase.query.coprocessor.AggregateClient;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateClientImpl;

/**
 * @since : 11/10/14,2014
 */
public class TestGroupAggregateTimeSeriesClient extends TestHBaseBase {

	private final static Logger LOG = LoggerFactory.getLogger(TestGroupAggregateTimeSeriesClient.class);

	HTableInterface table;
	long startTime;
	long endTime;
	List<String> rowkeys;
	AggregateClient client;
	Scan scan;
	EntityDefinition ed;

	@Before
	public void setUp() throws IllegalAccessException, InstantiationException {
		ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
		hbase.createTable("unittest", "f");
		table = EagleConfigFactory.load().getHTable("unittest");
		startTime = System.currentTimeMillis();
		try {
			rowkeys = prepareData(1000);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		endTime = System.currentTimeMillis();

		client = new AggregateClientImpl();
		scan = new Scan();
		ListQueryCompiler compiler = null;
		try {
			compiler = new ListQueryCompiler("TestTimeSeriesAPIEntity[@cluster=\"test4UT\" and @datacenter = \"dc1\"]{@field1,@field2}");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
		scan.setFilter(compiler.filter());
//		scan.setStartRow(EagleBase64Wrapper.decode(rowkeys.get(0)));
//		scan.setStopRow(EagleBase64Wrapper.decode(rowkeys.get(rowkeys.size()-1)));
	}

	private List<String> prepareData(int count) throws Exception {
		List<TestTimeSeriesAPIEntity> list = new ArrayList<TestTimeSeriesAPIEntity>();

		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestTimeSeriesAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
		}

		for(int i=0;i<count;i++){
			TestTimeSeriesAPIEntity e = new TestTimeSeriesAPIEntity();
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
		List<String> result = writer.write(list);
		return result;
	}


	//@Test
	public void testGroupTimeSeriesAggCountClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.count),Arrays.asList("count"),true,startTime,System.currentTimeMillis(),10).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("COUNT");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupTimeSeriesAggMaxClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.max),Arrays.asList("field2"),true,startTime,System.currentTimeMillis(),10).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MAX");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupTimeSeriesAggMinClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.min),Arrays.asList("field2"),true,startTime,System.currentTimeMillis(),10).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MIN");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupTimeSeriesAggAvgClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.min),Arrays.asList("field2"),true,startTime,System.currentTimeMillis(),10).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MIN");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupTimeSeriesAggSumClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan, Arrays.asList("cluster","datacenter"),Arrays.asList(AggregateFunctionType.sum),Arrays.asList("field2"),true,startTime,System.currentTimeMillis(),10).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("SUM");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void testGroupTimeSeriesAggMultipleClient(){
		try {
			List<GroupbyKeyValue> result = client.aggregate(table,ed,scan,
					Arrays.asList("cluster","datacenter"),
					Arrays.asList(AggregateFunctionType.max,AggregateFunctionType.min,AggregateFunctionType.avg,AggregateFunctionType.sum,AggregateFunctionType.count),
					Arrays.asList("field2","field2","field2","field2","field2"),true,startTime,System.currentTimeMillis(),16).getKeyValues();
			if(LOG.isDebugEnabled()) LOG.debug("MUTILPLE");
			logGroupbyKeyValue(result);
			Assert.assertNotNull(result);
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
			if(LOG.isDebugEnabled()) LOG.debug("KEY: " + keys + ", VALUE: " + vals);
		}
	}
}
