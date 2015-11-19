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
import org.apache.eagle.log.entity.GenericEntityBatchReader;
import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.service.hbase.EmbeddedHbase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestHBaseLogReader2 {
	private final static Logger LOG = LoggerFactory.getLogger(TestHBaseLogReader2.class);
    private static EmbeddedHbase hbase = EmbeddedHbase.getInstance();
	
	@SuppressWarnings("serial")
	@Test
	public void testStartTimeInclusiveEndTimeExclusive() throws IOException, IllegalAccessException, InstantiationException {
		EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
		hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinitionManager.registerEntity(TestTimeSeriesAPIEntity.class);
		try{
			final String cluster = "cluster1";
			final String datacenter = "dc1";
			String serviceName = "TestTimeSeriesAPIEntity";
			GenericEntityWriter writer = new GenericEntityWriter(serviceName);
			List<TestTimeSeriesAPIEntity> entities = new ArrayList<TestTimeSeriesAPIEntity>();
			TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
			long timestamp1 = DateTimeUtil.humanDateToSeconds("2014-04-08 03:00:00")*1000;
			LOG.info("First entity timestamp:" + timestamp1);
			entity.setTimestamp(timestamp1);
			entity.setTags(new HashMap<String, String>(){{
				put("cluster", cluster);
				put("datacenter", datacenter);
			}});
			entity.setField7("field7");
			entities.add(entity);
			
			entity = new TestTimeSeriesAPIEntity();
			long timestamp2 = DateTimeUtil.humanDateToSeconds("2014-05-08 04:00:00")*1000;
			LOG.info("Second entity timestamp:" + timestamp2);
			entity.setTimestamp(timestamp2);
			entity.setTags(new HashMap<String, String>(){{
				put("cluster", cluster);
				put("datacenter", datacenter);
			}});
			entity.setField7("field7_2");
			entities.add(entity);
			writer.write(entities);

			// for timezone difference between UTC & localtime, enlarge the search range
			long queryStartTimestamp = timestamp1-24*60*60*1000;
			long queryEndTimestamp = timestamp1+24*60*60*1000;
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
			Assert.assertEquals(timestamp1, list.get(0).getTimestamp());
			Assert.assertEquals("field7", list.get(0).getField7());

			// for timezone difference between UTC & localtime, enlarge the search range
			queryStartTimestamp = timestamp1-24*60*60*1000;
			queryEndTimestamp = timestamp2+24*60*60*1000;  // eagle timestamp is rounded to seconds
			condition.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryStartTimestamp));
			condition.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryEndTimestamp));
			reader = new GenericEntityBatchReader(serviceName, condition); 
			list = reader.read();
			Assert.assertEquals(2, list.size());
			
			queryStartTimestamp = timestamp1;
			queryEndTimestamp = timestamp1;  // eagle timestamp is rounded to seconds
			condition.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryStartTimestamp));
			condition.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(queryEndTimestamp));
			reader = new GenericEntityBatchReader(serviceName, condition); 
			list = reader.read();
			Assert.assertEquals(0, list.size());
			hbase.deleteTable(entityDefinition.getTable());
		}catch(Exception ex){
			LOG.error("error", ex);
			Assert.fail();
		}
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
