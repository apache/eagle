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
package org.apache.eagle.service.generic;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateClient;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateClientImpl;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestListQueryResource extends TestHBaseBase {

	HTableInterface table;
	long startTime;
	long endTime;
	List<String> rowkeys;
	AggregateClient client;
	Scan scan;
	String TEST_TIME_SERIES_API_SERVICE = "TestTimeSeriesAPIEntity";
	EntityDefinition entityDefinition = null;

	@Before
	public void setUp() throws IllegalAccessException, InstantiationException, IOException {
		entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(GenericMetricEntity.class);
		hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		table = EagleConfigFactory.load().getHTable("unittest");
		startTime = System.currentTimeMillis();
		try {
//			rowkeys = prepareTestEntity(200);
		} catch (Exception e) {
			e.printStackTrace();
			junit.framework.Assert.fail(e.getMessage());
		}
		endTime = System.currentTimeMillis();

		client = new AggregateClientImpl();
		scan = new Scan();
		ListQueryCompiler compiler = null;
		try {
			compiler = new ListQueryCompiler(TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" and @datacenter = \"dc1\"]{@field1,@field2}");
		} catch (Exception e) {
			junit.framework.Assert.fail(e.getMessage());
		}
		scan.setFilter(compiler.filter());
//		scan.setStartRow(EagleBase64Wrapper.decode(rowkeys.get(0)));
//		scan.setStopRow(EagleBase64Wrapper.decode(rowkeys.get(rowkeys.size()-1)));
	}

	@After
	public void cleanUp() throws IllegalAccessException, InstantiationException, IOException {
		entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(GenericMetricEntity.class);
		hbase.deleteTable(entityDefinition.getTable());

		entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		hbase.deleteTable(entityDefinition.getTable());
	}

	@SuppressWarnings("unused")
	private List<String> prepareTestEntity(int count) throws Exception {
		List<TestTimeSeriesAPIEntity> list = new ArrayList<TestTimeSeriesAPIEntity>();
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);

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

	private List<String> prepareMetricEntity(long startTime,int count) throws Exception {
		List<GenericMetricEntity> list = new ArrayList<GenericMetricEntity>();
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(GenericMetricEntity.class);

		double[] value = new double[60];
		for(int i=0;i<60;i++){
			value[i] = 1;
		}
		for(int i=0;i<count;i++){
			GenericMetricEntity e = new GenericMetricEntity();
			e.setTimestamp(startTime+i*3600*1000);
			e.setValue(value);
			e.setTags(new HashMap<String, String>());
			e.getTags().put("cluster", "test4UT");
			e.getTags().put("datacenter", "dc1");
			e.getTags().put("index", ""+i);
			e.getTags().put("jobId", "job_"+System.currentTimeMillis());
			e.setPrefix("eagle.metric.test");
			list.add(e);
		}

		GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
		List<String> result = writer.write(list);
		return result;
	}

	@Test
	public void testMetricQuery() throws Exception {
		long _startTime = System.currentTimeMillis();
		List<String> rowKeys = prepareMetricEntity(startTime,200);
		long _endTime = startTime+ 200 * 3600*1000;
		String startTime = DateTimeUtil.secondsToHumanDate(_startTime / 1000);
		String endTime = DateTimeUtil.secondsToHumanDate((_endTime + 1000) / 1000);

		ListQueryResource resource = new ListQueryResource();
		String query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";

		ListQueryAPIResponseEntity response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]{EXP{@value/3}}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]{*}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());


		//////////////////////////////////////////
		/// Time series aggregation
		//////////////////////////////////////////
		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());
	}

	@Test
	public void testMetricQueryWithSort() throws Exception {
		long _startTime = System.currentTimeMillis();
		List<String> rowKeys = prepareMetricEntity(startTime,200);
		long _endTime = startTime+ 200 * 3600*1000;
		String startTime = DateTimeUtil.secondsToHumanDate(_startTime / 1000);
		String endTime = DateTimeUtil.secondsToHumanDate((_endTime + 1000) / 1000);

		ListQueryResource resource = new ListQueryResource();
		String query;

		ListQueryAPIResponseEntity response;


		//////////////////////////////////////////
		/// Time series aggregation
		//////////////////////////////////////////
		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(value)}.{avg(value) desc}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(value)}.{sum(value) desc}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(value)}.{max(value) asc}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 6000000, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "GenericMetricService[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(value)}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, false, 1, 0, false, 0, "eagle.metric.test");
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());
	}

	@Test
	public void testPartitionBasedQuery() throws InstantiationException, IllegalAccessException {
		String[] partitions =  new String[2];
		partitions[0] = "cluster";
		partitions[1] = "datacenter";
		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		EntityDefinition entityDef = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
		entityDef.setPartitions(partitions);
		entityDef.setTimeSeries(true);
		
		ListQueryResource resource = new ListQueryResource();
		String query = "TestLogAPIEntity[]{@cluster}";
		String startTime = DateTimeUtil.secondsToHumanDate(System.currentTimeMillis() / 1000);
		String endTime = DateTimeUtil.secondsToHumanDate((System.currentTimeMillis() + 1000) / 1000);
		
		ListQueryAPIResponseEntity response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertFalse(response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" ]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertFalse(response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\"]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());
		
		query = "TestLogAPIEntity[(@cluster=\"cluster1\") OR (@cluster=\"cluster1\" AND @datacenter=\"dc1\")]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertFalse(response.isSuccess());

		query = "TestLogAPIEntity[(@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @tag1=\"value1\") OR (@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @tag2=\"value2\")]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@tag1=\"value1\" OR @tag2=\"value2\")]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		// Tag with =, !=, =~, !=~, contains, not contains, in, not in
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@tag1!=\"value1\" or @tag2=~\"value2\" or ((@tag3 contains \"value3\") or (@tag3 not contains \"value3\")) or @tag4 !=~ \"value4\" or @tag5 in (\"value5-1\",\"value5-2\",\"value5-3\") or @tag6 not in (\"value5-1\",\"value5-2\",\"value5-3\"))]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		// Tag with complex IN & List
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@tag5 in (\"value5-1\",\"value5,2\"))]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue(response.isSuccess());

		// Tag with unsupported operation
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@tag1 < \"value1\")]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertFalse("Tag with unsupported operation, should get exception",response.isSuccess());

		// Field with numeric
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND " +
				"(@field1 < 1 or @field2 = 2 or @field3 = 13456789 or @field4 = 987654321 or @field5 = 5.6 or @field7 < \"value7\")]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Field with numeric, should success",response.isSuccess());

		// Field with not supported negative numeric value which is to be supported with coprocessor later
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@field1 < -1)]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Field supporte negative numeric value, should get exception", response.isSuccess());

		// Query with escaped value
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND (@tag1 in (\"\\\"value1-part1\\\",\\\"value1-part2\\\"\",\"value2\"))]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support escaped quotes, should success: "+query,response.isSuccess());

		// Query with null
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @tag1 is null ]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @tag1 is not null]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is not null, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @tag1 is not null]{*}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is not null, should success: "+query,response.isSuccess());

//		startTime = "2014-02-13 15:00:00";
//		endTime = "2014-02-13 15:05:00";
//		query = "TaskAttemptExecutionService[@cluster=\"cluster1\" AND @datacenter=\"dc1\"]{@cluster,@startTime}";
//		response = resource.listQuery(query, startTime, endTime, 10, null, false, true, 1);
//		Assert.assertNotNull(response);
//		Assert.assertTrue(response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND EXP{@field1 + @field2} > 0]{@cluster}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support expression in filter, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND EXP{@field1 + @field2} > 0]{EXP{@field1 + @field2} }";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support expression in output, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND EXP{@field1 + @field2} > 0]{EXP{@field1 + @field2} as A}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support expression output with alias, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND EXP{@field1 + @field2} > 0]{@cluster,EXP{@field1 + @field2} as A}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null,true);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support parameter \"verbose\" is true, should success: "+query,response.isSuccess());

		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND EXP{@field1 + @field2} > 0]{@cluster,EXP{@field1 + @field2} as A}";
		response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null,false);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support parameter \"verbose\" is false, should success: "+query,response.isSuccess());
	}

	@Test
	public void testObjectTypeFieldQuery() throws IllegalAccessException, InstantiationException {

		String startTime = DateTimeUtil.secondsToHumanDate(System.currentTimeMillis() / 1000);
		String endTime = DateTimeUtil.secondsToHumanDate((System.currentTimeMillis() + 1000) / 1000);

		String[] partitions =  new String[2];
		partitions[0] = "cluster";
		partitions[1] = "datacenter";
		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		EntityDefinition entityDef = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
		entityDef.setPartitions(partitions);
		entityDef.setTimeSeries(true);

		ListQueryResource resource = new ListQueryResource();
		String query = "TestLogAPIEntity[]{@cluster}";

		// Field with numeric
		query = "TestLogAPIEntity[@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND " +
				"(@field1 < 1 or @field2 = 2 or @field3 = 13456789 or @field4 = 987654321 or @field5 = 5.6 or @field7 < \"value7\")]{@cluster}";
		ListQueryAPIResponseEntity response = resource.listQuery(query, startTime, endTime, 100, null, false, true, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Field with numeric, should success",response.isSuccess());
	}

	@Test
	public void testFlatAggregateQuery(){
		ListQueryResource resource = new ListQueryResource();
		ListQueryAPIResponseEntity response = null;
		String query = null;
//		String start = DateTimeUtil.secondsToHumanDate(startTime / 1000);
		String start = DateTimeUtil.secondsToHumanDate(0);

		String end = DateTimeUtil.secondsToHumanDate((endTime + 24 * 3600) / 1000);

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		
		Assert.assertNotNull(response);
//		Assert.assertTrue(response.getLastTimestamp() > 0);
//		Assert.assertTrue(response.getFirstTimestamp() >= 0);
//		Assert.assertTrue(response.getLastTimestamp() >= response.getFirstTimestamp());
		
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(field1)}.{max(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(field1)}.{min(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(field1)}.{sum(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(field1)}.{avg(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}.{count desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}.{sum(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}.{count desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, false, 1, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());

//		ArrayList<Map.Entry<List<String>,List<Double>>> obj = (ArrayList<Map.Entry<List<String>,List<Double>>>) response.getObj();
//		Assert.assertEquals(1,obj.size());
//		Assert.assertEquals(2,obj.get(0).getKey().size());
//		Assert.assertEquals("test4UT",obj.get(0).getKey().get(0));
//		Assert.assertEquals("dc1",obj.get(0).getKey().get(1));
//		Assert.assertEquals(new Double(1.0),obj.get(0).getValue().get(0));
//		Assert.assertEquals(new Double(1.0),obj.get(0).getValue().get(1));
//		Assert.assertEquals(new Double(1.0), obj.get(0).getValue().get(2));
//		Assert.assertTrue(obj.get(0).getValue().get(3) > 1.0);
	}

	/**
	 * TODO: Add time series aggregation query unit test
	 */
	@Test
	public void testTimeSeriesAggregateQuery(){
		ListQueryResource resource = new ListQueryResource();
		ListQueryAPIResponseEntity response = null;
		String query = null;
		String start = DateTimeUtil.secondsToHumanDate(startTime / 1000);
		String end = DateTimeUtil.secondsToHumanDate((endTime + 1000) / 1000);

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(field1)}.{max(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{min(field1)}.{min(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{sum(field1)}.{sum(field1) desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{avg(field1)}.{avg(field1) asc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}.{max(field1) asc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}.{count desc}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 16, 1000, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());
	}
	/**
	 * TODO: Add time series aggregation query unit test
	 */
	@Test
	public void testTimeSeriesAggregateQueryWithoutCoprocessor(){
		ListQueryResource resource = new ListQueryResource();
		ListQueryAPIResponseEntity response = null;
		String query = null;
		String start = DateTimeUtil.secondsToHumanDate(startTime / 1000);
		String end = DateTimeUtil.secondsToHumanDate((endTime + 1000) / 1000);

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{count}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 10, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster>{max(field1)}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 10, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: "+query,response.isSuccess());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}";
		response = resource.listQuery(query, start, end, 100000, null, false, true, 10, 0, false, 0, null);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());

		query = TEST_TIME_SERIES_API_SERVICE +"[@cluster=\"test4UT\" AND @datacenter=\"dc1\"]<@cluster,@datacenter>{max(field1),min(field1),avg(field1),sum(field1),count}.{max(field1) desc}";
		response = resource.listQueryWithoutCoprocessor(query, start, end, 100000, null, false, true, 10, 1000, false, 0, null,false);
		Assert.assertNotNull(response);
		Assert.assertTrue("Support is null, should success: " + query, response.isSuccess());
//		Assert.assertEquals(1, response.getTotalResults());
	}
}
