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

import org.apache.eagle.log.entity.index.NonClusteredIndexStreamReader;
import org.apache.eagle.log.entity.index.UniqueIndexStreamReader;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.log.entity.old.GenericDeleter;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.query.parser.EagleQueryParser;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestGenericEntityIndexStreamReader extends TestHBaseBase {

	@Test
	public void testUniqueIndexRead() throws Exception {
		EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		
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
		e.getTags().put("jobID", "index_test_job_id");
		e.getTags().put("hostname", "testhost");
		list.add(e);

		GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
		List<String> result = writer.write(list);
		Assert.assertNotNull(result);
		
		IndexDefinition indexDef = ed.getIndexes()[0];
		SearchCondition condition = new SearchCondition();
		condition.setOutputFields(new ArrayList<String>());
		condition.getOutputFields().add("field1");
		condition.getOutputFields().add("field2");
		condition.getOutputFields().add("field3");
		condition.getOutputFields().add("field4");
		condition.getOutputFields().add("field5");
		condition.getOutputFields().add("field6");
		condition.getOutputFields().add("field7");

		String query = "@field7 = \"7\" AND @jobID = \"index_test_job_id\" ";
		EagleQueryParser parser = new EagleQueryParser(query);
		condition.setQueryExpression(parser.parse());

		UniqueIndexStreamReader indexReader = new UniqueIndexStreamReader(indexDef, condition);
		GenericEntityBatchReader batchReader = new GenericEntityBatchReader(indexReader);
		List<TestLogAPIEntity> entities =  batchReader.read();
		Assert.assertNotNull(entities);
		Assert.assertTrue(entities.size() >= 1);
		TestLogAPIEntity e1 = entities.get(0);
		Assert.assertEquals(e.getField1(), e1.getField1());
		Assert.assertEquals(e.getField2(), e1.getField2());
		Assert.assertEquals(e.getField3(), e1.getField3());
		Assert.assertEquals(e.getField4(), e1.getField4());
		Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
		Assert.assertEquals(e.getField6(), e1.getField6());
		Assert.assertEquals(e.getField7(), e1.getField7());
		
		GenericDeleter deleter = new GenericDeleter(ed.getTable(), ed.getColumnFamily());
		deleter.delete(list);
		
		indexReader = new UniqueIndexStreamReader(indexDef, condition);
		batchReader = new GenericEntityBatchReader(indexReader);
		entities =  batchReader.read();
		hbase.deleteTable(entityDefinition.getTable());
		Assert.assertNotNull(entities);
		Assert.assertTrue(entities.isEmpty());
	}

	@Test
	public void testNonClusterIndexRead() throws Exception {
        EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		
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
		e.getTags().put("jobID", "index_test_job_id");
		e.getTags().put("hostname", "testhost");
		list.add(e);

		GenericEntityWriter writer = new GenericEntityWriter(ed.getService());
		List<String> result = writer.write(list);
		Assert.assertNotNull(result);
		
		IndexDefinition indexDef = ed.getIndexes()[1];
		SearchCondition condition = new SearchCondition();
		condition.setOutputFields(new ArrayList<String>());
		condition.getOutputFields().add("field1");
		condition.getOutputFields().add("field2");
		condition.getOutputFields().add("field3");
		condition.getOutputFields().add("field4");
		condition.getOutputFields().add("field5");
		condition.getOutputFields().add("field6");
		condition.getOutputFields().add("field7");

		String query = "@field7 = \"7\" AND @jobID = \"index_test_job_id\" AND @hostname = \"testhost\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		condition.setQueryExpression(parser.parse());

		NonClusteredIndexStreamReader indexReader = new NonClusteredIndexStreamReader(indexDef, condition);
		GenericEntityBatchReader batchReader = new GenericEntityBatchReader(indexReader);
		List<TestLogAPIEntity> entities =  batchReader.read();
		Assert.assertNotNull(entities);
		Assert.assertTrue(entities.size() >= 1);
		TestLogAPIEntity e1 = entities.get(0);
		Assert.assertEquals(e.getField1(), e1.getField1());
		Assert.assertEquals(e.getField2(), e1.getField2());
		Assert.assertEquals(e.getField3(), e1.getField3());
		Assert.assertEquals(e.getField4(), e1.getField4());
		Assert.assertEquals(e.getField5(), e1.getField5(), 0.001);
		Assert.assertEquals(e.getField6(), e1.getField6());
		Assert.assertEquals(e.getField7(), e1.getField7());


		GenericDeleter deleter = new GenericDeleter(ed.getTable(), ed.getColumnFamily());
		deleter.delete(list);
		
		indexReader = new NonClusteredIndexStreamReader(indexDef, condition);
		batchReader = new GenericEntityBatchReader(indexReader);
		entities =  batchReader.read();
		hbase.deleteTable(entityDefinition.getTable());
		Assert.assertNotNull(entities);
		Assert.assertTrue(entities.isEmpty());
	}
}
