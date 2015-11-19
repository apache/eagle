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

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.service.hbase.TestHBaseBase;
import junit.framework.Assert;
import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class TestHBaseWriteEntitiesPerformance extends TestHBaseBase {
	private EntityDefinition ed;
	private final static Logger LOG = LoggerFactory.getLogger(TestHBaseWriteEntitiesPerformance.class);

	@Before
	public void setUp() throws IllegalAccessException, InstantiationException, IOException {
		EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());

		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		try {
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
			ed.setTimeSeries(true);
		} catch (InstantiationException | IllegalAccessException e) {
			Assert.fail(e.getMessage());
		}
    }

	@After
	public void cleanUp() throws IllegalAccessException, InstantiationException, IOException {
		EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
		hbase.deleteTable(entityDefinition.getTable());
	}

	private List<String> writeEntities(int count){
		GenericEntityWriter writer = null;
		try {
			writer = new GenericEntityWriter(ed.getService());
		} catch (InstantiationException e1) {
			Assert.fail(e1.getMessage());
		} catch (IllegalAccessException e1) {
			Assert.fail(e1.getMessage());
		}

		if(LOG.isDebugEnabled()) LOG.debug("Start to write "+count+" entities");
		int wroteCount = 0;
		List<String> rowkeys = new ArrayList<String>();
		List<TestLogAPIEntity> list = new ArrayList<TestLogAPIEntity>();
		for(int i=0;i<= count;i++){
			TestLogAPIEntity e = new TestLogAPIEntity();
			e.setTimestamp(new Date().getTime());
			e.setField1(i);
			e.setField2(i);
			e.setField3(i);
			e.setField4(new Long(i));
			e.setField5(new Double(i));
			e.setField6(new Double(i));
			e.setField7(String.valueOf(i));
			e.setTags(new HashMap<String, String>());
			e.getTags().put("jobID", "index_test_job_id");
			e.getTags().put("hostname", "testhost");
			e.getTags().put("index", String.valueOf(i));
			e.getTags().put("class", e.toString());
			list.add(e);

			if(list.size()>=1000){
				try {
					StopWatch watch = new StopWatch();
					watch.start();
					rowkeys.addAll(writer.write(list));
					watch.stop();
					wroteCount += list.size();
					if(LOG.isDebugEnabled()) LOG.debug("Wrote "+wroteCount+" / "+count+" entities"+" in "+watch.getTime()+" ms");
					list.clear();
				} catch (Exception e1) {
					Assert.fail(e1.getMessage());
				}
			}
		}

		try {
			rowkeys.addAll(writer.write(list));
			wroteCount += list.size();
			if(LOG.isDebugEnabled()) LOG.debug("wrote "+wroteCount+" / "+count+" entities");
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
		if(LOG.isDebugEnabled()) LOG.debug("done "+count+" entities");
		return rowkeys;
	}

	@SuppressWarnings("unused")
	@Test
	public void testWrite1MLogAPIEntities(){
		Date startTime = new Date();
		LOG.info("Start time: " + startTime);
		StopWatch watch = new StopWatch();
		watch.start();
		List<String> rowKeys = writeEntities(10);
		Assert.assertNotNull(rowKeys);
		watch.stop();
		Date endTime = new Date();
		LOG.info("End time: " + endTime);
		LOG.info("Totally take " + watch.getTime() * 1.0 / 1000 + " s");
	}
}