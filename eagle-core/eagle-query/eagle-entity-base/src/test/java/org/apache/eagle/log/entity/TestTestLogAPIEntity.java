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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.index.UniqueIndexLogReader;
import org.apache.eagle.log.entity.meta.EntityConstants;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.log.entity.old.GenericDeleter;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestTestLogAPIEntity extends TestHBaseBase {

	@Test 
	public void testGetValue() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}

		Assert.assertNotNull(ed);
		Assert.assertNotNull(ed.getQualifierGetterMap());
		TestLogAPIEntity e = new TestLogAPIEntity();
		e.setField1(1);
		e.setField2(2);
		e.setField3(3);
		e.setField4(4L);
		e.setField5(5.0);
		e.setField6(6.0);
		e.setField7("7");
		e.setTags(new HashMap<String, String>());
		e.getTags().put("tag1", "value1");

		Assert.assertNotNull(ed.getQualifierGetterMap().get("field1"));
		Assert.assertEquals(1, ed.getValue(e, "field1"));
		Assert.assertEquals(2, ed.getValue(e, "field2"));
		Assert.assertEquals(3L, ed.getValue(e, "field3"));
		Assert.assertEquals(4L, ed.getValue(e, "field4"));
		Assert.assertEquals(5.0, ed.getValue(e, "field5"));
		Assert.assertEquals(6.0, ed.getValue(e, "field6"));
		Assert.assertEquals("7", ed.getValue(e, "field7"));
		Assert.assertEquals("value1", ed.getValue(e, "tag1"));
	}
	
	@Test
	public void testIndexDefinition() throws InstantiationException, IllegalAccessException {
		
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}
		Assert.assertNotNull(ed);
		IndexDefinition[] indexDefinitions = ed.getIndexes();
		Assert.assertNotNull(indexDefinitions);
		Assert.assertEquals(2, indexDefinitions.length);
		for (IndexDefinition def : indexDefinitions) {
			Assert.assertNotNull(def.getIndexName());
			Assert.assertNotNull(def.getIndexColumns());
			Assert.assertEquals(1, def.getIndexColumns().length);
		}
	}
	
	@Test
	public void testWriteEmptyIndexFieldAndDeleteWithoutPartition() throws Exception {
		EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}
		String[] partitions = ed.getPartitions();
		ed.setPartitions(null);
		
		try {
			List<TestLogAPIEntity> list = new ArrayList<TestLogAPIEntity>();
			TestLogAPIEntity e = new TestLogAPIEntity();
			e.setField1(1);
			e.setField2(2);
			e.setField3(3);
			e.setField4(4L);
			e.setField5(5.0);
			e.setField6(5.0);
			e.setField7("7");
			e.setTags(new HashMap<String, String>());
            e.getTags().put("tag1", "value1");
			list.add(e);
	
			GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
			List<String> result = writer.write(list);
			Assert.assertNotNull(result);
			
			List<byte[]> indexRowkeys = new ArrayList<byte[]>();
			IndexDefinition[] indexDefs = ed.getIndexes();
			for (IndexDefinition index : indexDefs) {
				byte[] indexRowkey = index.generateIndexRowkey(e);
				indexRowkeys.add(indexRowkey);
			}
			byte[][] qualifiers = new byte[7][];
			qualifiers[0] = "a".getBytes();
			qualifiers[1] = "b".getBytes();
			qualifiers[2] = "c".getBytes();
			qualifiers[3] = "d".getBytes();
			qualifiers[4] = "e".getBytes();
			qualifiers[5] = "f".getBytes();
			qualifiers[6] = "g".getBytes();
			
			UniqueIndexLogReader reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			InternalLog log = reader.read();
			Assert.assertNotNull(log);
	
			TaggedLogAPIEntity newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			TestLogAPIEntity e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			
			log = reader.read();
			Assert.assertNotNull(log);
			newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			
			log = reader.read();
			Assert.assertNull(log);
			reader.close();
	
			GenericDeleter deleter = new GenericDeleter(ed.getTable(), ed.getColumnFamily());
			deleter.delete(list);
			
			reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			log = reader.read();
			Assert.assertNull(log);
			reader.close();
		} finally {
			ed.setPartitions(partitions);
		}
		hbase.deleteTable(entityDefinition.getTable());
	}
	

	/*
	 *  testWriteEmptyIndexFieldAndDeleteWithPartition(eagle.log.entity.TestTestLogAPIEntity): expected:<86400000> but was:<0>
	 */
	//@Test
	public void testWriteEmptyIndexFieldAndDeleteWithPartition() throws Exception {
        EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}
		String[] partitions = ed.getPartitions();
		String[] newPart = new String[2];
		newPart[0] = "cluster";
		newPart[1] = "datacenter";
		ed.setPartitions(newPart);
		
		try {
			List<TestLogAPIEntity> list = new ArrayList<TestLogAPIEntity>();
			TestLogAPIEntity e = new TestLogAPIEntity();
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
			list.add(e);
	
			GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
			List<String> result = writer.write(list);
			Assert.assertNotNull(result);
			
			List<byte[]> indexRowkeys = new ArrayList<byte[]>();
			IndexDefinition[] indexDefs = ed.getIndexes();
			for (IndexDefinition index : indexDefs) {
				byte[] indexRowkey = index.generateIndexRowkey(e);
				indexRowkeys.add(indexRowkey);
			}
			byte[][] qualifiers = new byte[9][];
			qualifiers[0] = "a".getBytes();
			qualifiers[1] = "b".getBytes();
			qualifiers[2] = "c".getBytes();
			qualifiers[3] = "d".getBytes();
			qualifiers[4] = "e".getBytes();
			qualifiers[5] = "f".getBytes();
			qualifiers[6] = "g".getBytes();
			qualifiers[7] = "cluster".getBytes();
			qualifiers[8] = "datacenter".getBytes();
			
			UniqueIndexLogReader reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			InternalLog log = reader.read();
			Assert.assertNotNull(log);
	
			TaggedLogAPIEntity newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			TestLogAPIEntity e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			Assert.assertEquals("test4UT", e1.getTags().get("cluster"));
			Assert.assertEquals("dc1", e1.getTags().get("datacenter"));
			Assert.assertEquals(EntityConstants.FIXED_WRITE_TIMESTAMP, e1.getTimestamp());

			log = reader.read();
			Assert.assertNotNull(log);
			newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			Assert.assertEquals("test4UT", e1.getTags().get("cluster"));
			Assert.assertEquals("dc1", e1.getTags().get("datacenter"));
			Assert.assertEquals(EntityConstants.FIXED_WRITE_TIMESTAMP, e1.getTimestamp());

			log = reader.read();
			Assert.assertNull(log);
			reader.close();

			GenericDeleter deleter = new GenericDeleter(ed.getTable(), ed.getColumnFamily());
			deleter.delete(list);
			
			reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			log = reader.read();
			Assert.assertNull(log);
			reader.close();
		} finally {
			ed.setPartitions(partitions);
		}
		hbase.deleteTable(entityDefinition.getTable());
	}

	/**
	 * testWriteEmptyIndexFieldAndDeleteWithPartitionAndTimeSeries(eagle.log.entity.TestTestLogAPIEntity): expected:<1434809555569> but was:<0>
	 */
	
	//@Test
	public void testWriteEmptyIndexFieldAndDeleteWithPartitionAndTimeSeries() throws Exception {
        EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		if (ed == null) {
			EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		}
		String[] partitions = ed.getPartitions();
		String[] newPart = new String[2];
		newPart[0] = "cluster";
		newPart[1] = "datacenter";
		ed.setPartitions(newPart);
		boolean isTimeSeries = ed.isTimeSeries();
		ed.setTimeSeries(true);
		long now = System.currentTimeMillis();
		
		try {
			List<TestLogAPIEntity> list = new ArrayList<TestLogAPIEntity>();
			TestLogAPIEntity e = new TestLogAPIEntity();
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
			e.setTimestamp(now);
			list.add(e);
	
			GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
			List<String> result = writer.write(list);
			Assert.assertNotNull(result);
			
			List<byte[]> indexRowkeys = new ArrayList<byte[]>();
			IndexDefinition[] indexDefs = ed.getIndexes();
			for (IndexDefinition index : indexDefs) {
				byte[] indexRowkey = index.generateIndexRowkey(e);
				indexRowkeys.add(indexRowkey);
			}
			byte[][] qualifiers = new byte[9][];
			qualifiers[0] = "a".getBytes();
			qualifiers[1] = "b".getBytes();
			qualifiers[2] = "c".getBytes();
			qualifiers[3] = "d".getBytes();
			qualifiers[4] = "e".getBytes();
			qualifiers[5] = "f".getBytes();
			qualifiers[6] = "g".getBytes();
			qualifiers[7] = "cluster".getBytes();
			qualifiers[8] = "datacenter".getBytes();
			
			UniqueIndexLogReader reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			InternalLog log = reader.read();
			Assert.assertNotNull(log);
	
			TaggedLogAPIEntity newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			TestLogAPIEntity e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			Assert.assertEquals("test4UT", e1.getTags().get("cluster"));
			Assert.assertEquals("dc1", e1.getTags().get("datacenter"));
			Assert.assertEquals(now, e1.getTimestamp());

			log = reader.read();
			Assert.assertNotNull(log);
			newEntity = HBaseInternalLogHelper.buildEntity(log, ed);
			Assert.assertEquals(TestLogAPIEntity.class, newEntity.getClass());
			e1 = (TestLogAPIEntity)newEntity;
			Assert.assertEquals(e.getField1(), e1.getField1());
			Assert.assertEquals(e.getField2(), e1.getField2());
			Assert.assertEquals(e.getField3(), e1.getField3());
			Assert.assertEquals(e.getField4(), e1.getField4());
			Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
			Assert.assertEquals(e.getField6(), e1.getField6());
			Assert.assertEquals(e.getField7(), e1.getField7());
			Assert.assertEquals("test4UT", e1.getTags().get("cluster"));
			Assert.assertEquals("dc1", e1.getTags().get("datacenter"));
			Assert.assertEquals(now, e1.getTimestamp());

			log = reader.read();
			Assert.assertNull(log);
			reader.close();

			GenericDeleter deleter = new GenericDeleter(ed.getTable(), ed.getColumnFamily());
			deleter.delete(list);
			
			reader = new UniqueIndexLogReader(indexDefs[0], indexRowkeys, qualifiers, null);
			reader.open();
			log = reader.read();
			Assert.assertNull(log);
			reader.close();
		} finally {
			ed.setPartitions(partitions);
			ed.setTimeSeries(isTimeSeries);
		}
		hbase.deleteTable(entityDefinition.getTable());
	}

}
