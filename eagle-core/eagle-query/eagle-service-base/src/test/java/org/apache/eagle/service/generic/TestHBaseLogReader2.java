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
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.*;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


@RunWith(MockitoJUnitRunner.class)
public class TestHBaseLogReader2 {
	private final static Logger LOG = LoggerFactory.getLogger(TestHBaseLogReader2.class);
	@Mock
	private static HBaseTestingUtility hBaseTestingUtility;
	@Mock
	private static EagleConfigFactory eagleConfigFactory;
	@Mock
	private static HTable hTable;
	@Mock
	private static ResultScanner resultScanner;
	private Queue<Result> queue;
	private static Result result1;
	private static Result result2;
	private static long timestamp1;
	private static long timestamp2;
	private final static String cluster = "cluster1";
	private final static String datacenter = "dc1";
	private static String serviceName;
	private static EntityDefinition entityDefinition;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
		EntityDefinitionManager.registerEntity(TestTimeSeriesAPIEntity.class);
		serviceName = "TestTimeSeriesAPIEntity";

		TestTimeSeriesAPIEntity entityA = new TestTimeSeriesAPIEntity();
		timestamp1 = DateTimeUtil.humanDateToSeconds("2014-04-08 03:00:00") * 1000;
		LOG.info("First entity timestamp:" + timestamp1);
		entityA.setTimestamp(timestamp1);
		entityA.setTags(new HashMap<String, String>() {{
			put("cluster", cluster);
			put("datacenter", datacenter);
		}});
		entityA.setField7("field7");

		TestTimeSeriesAPIEntity entityB = new TestTimeSeriesAPIEntity();
		timestamp2 = DateTimeUtil.humanDateToSeconds("2014-05-08 04:00:00") * 1000;
		LOG.info("Second entity timestamp:" + timestamp2);
		entityB.setTimestamp(timestamp2);
		entityB.setTags(new HashMap<String, String>() {{
			put("cluster", cluster);
			put("datacenter", datacenter);
		}});
		entityB.setField7("field7_2");

		List<Cell> cellsList1 = new ArrayList<>();
		Cell cell1 = CellUtil.createCell(generateRow(entityA, entityDefinition), Bytes.toBytes(entityDefinition.getColumnFamily()), Bytes.toBytes("g"), System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("field7"));
		cellsList1.add(cell1);
		result1 = Result.create(cellsList1);

		List<Cell> cellsList2 = new ArrayList<>();
		Cell cell2 = CellUtil.createCell(generateRow(entityB, entityDefinition), Bytes.toBytes(entityDefinition.getColumnFamily()), Bytes.toBytes("g"), System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("field7_2"));
		cellsList2.add(cell2);
		result2 = Result.create(cellsList2);
	}

	@SuppressWarnings("serial")
	@Test
	public void testStartTimeInclusiveEndTimeExclusive() throws Exception, IllegalAccessException, InstantiationException {
		Whitebox.setInternalState(EagleConfigFactory.class, "manager", eagleConfigFactory);
		PowerMockito.when(eagleConfigFactory.getHTable(Mockito.any(String.class))).thenReturn(hTable);
		PowerMockito.when(eagleConfigFactory.getTimeZone()).thenReturn(TimeZone.getTimeZone(EagleConfigConstants.DEFAULT_EAGLE_TIME_ZONE));
		PowerMockito.when(hTable.getScanner(Mockito.any(Scan.class))).thenReturn(resultScanner);

		try {
			queue = new LinkedList<>();
			queue.add(result1);
			PowerMockito.when(resultScanner.next()).thenReturn(queue.poll(), null);
			long queryStartTimestamp = timestamp1 - 24 * 60 * 60 * 1000;
			long queryEndTimestamp = timestamp1 + 24 * 60 * 60 * 1000;
			LOG.info("Query start timestamp:" + queryStartTimestamp);
			LOG.info("Query end  timestamp:" + queryEndTimestamp);
			String format = "%s[@cluster=\"%s\" AND @datacenter=\"%s\"]{%s}";
			String query = String.format(format, serviceName, cluster, datacenter, "@field7");
			ListQueryCompiler comp = new ListQueryCompiler(query);
			SearchCondition condition = new SearchCondition();
			condition.setFilter(comp.filter());
			condition.setQueryExpression(comp.getQueryExpression());
			condition.setOutputFields(comp.outputFields());
			final List<String[]> partitionValues = comp.getQueryPartitionValues();
			if (partitionValues != null) {
				condition.setPartitionValues(Arrays.asList(partitionValues.get(0)));
			}
			condition.setStartRowkey(null);
			condition.setPageSize(Integer.MAX_VALUE);
			condition.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(0));
			condition.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryEndTimestamp));
			GenericEntityBatchReader reader = new GenericEntityBatchReader(serviceName, condition);
			List<TestTimeSeriesAPIEntity> list = reader.read();
			Assert.assertEquals(1, list.size());
			Assert.assertEquals("field7", list.get(0).getField7());
			Assert.assertEquals(timestamp1, list.get(0).getTimestamp());


			queue = new LinkedList<>();
			queue.add(result1);
			queue.add(result2);
			PowerMockito.when(resultScanner.next()).thenReturn(queue.poll(), queue.poll(), null);
			// for timezone difference between UTC & localtime, enlarge the search range
			queryStartTimestamp = timestamp1 - 24 * 60 * 60 * 1000;
			queryEndTimestamp = timestamp2 + 24 * 60 * 60 * 1000;  // eagle timestamp is rounded to seconds
			condition.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryStartTimestamp));
			condition.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryEndTimestamp));
			reader = new GenericEntityBatchReader(serviceName, condition);
			list = reader.read();
			Assert.assertEquals(2, list.size());

			PowerMockito.when(resultScanner.next()).thenReturn(null);
			queryStartTimestamp = timestamp1;
			queryEndTimestamp = timestamp1;  // eagle timestamp is rounded to seconds
			condition.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryStartTimestamp));
			condition.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryEndTimestamp));
			reader = new GenericEntityBatchReader(serviceName, condition);
			list = reader.read();
			Assert.assertEquals(0, list.size());


		} catch (Exception ex) {
			LOG.error("error", ex);
			Assert.fail();
		} finally {
			hBaseTestingUtility.deleteTable(hTable.getTableName());

		}

	}

	private static byte[] generateRow(TestTimeSeriesAPIEntity entity, EntityDefinition entityDefinition) throws Exception {
		InternalLog entityLog = HBaseInternalLogHelper.convertToInternalLog(entity, entityDefinition);
		return RowkeyBuilder.buildRowkey(entityLog);
	}

	@Test
	public void testByteComparison(){
		byte[] byte1 = new byte[]{-23, 12, 63};
		byte[] byte2 = ByteUtil.concat(byte1, new byte[]{0});
		Assert.assertTrue(Bytes.compareTo(byte1, byte2) < 0);
		byte[] byte3 = ByteUtil.concat(byte1, new byte[]{127});
		Assert.assertTrue(Bytes.compareTo(byte2, byte3) < 0);
		byte[] byte4 = ByteUtil.concat(byte1, new byte[]{-128});
		Assert.assertTrue(Bytes.compareTo(byte4, byte3) > 0);
	}

	@Test
	public void testMaxByteInBytesComparision(){
		int max = -1000000;
//		int maxb = -1000000;
		System.out.println("Byte MaxValue: " + Byte.MAX_VALUE);
		System.out.println("Byte MaxValue: " + Byte.MIN_VALUE);
		for(int i=-128; i<128; i++){
			byte b = (byte)i;
			int tmp = b & 0xff;
			max = Math.max(max, tmp);
		}
		System.out.println(max);

		byte b = -1;
		System.out.println(b & 0xff);
	}
}