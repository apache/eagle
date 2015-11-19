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
package org.apache.eagle.alert.dao;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.siddhi.StreamMetadataManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStreamDefinitionDAOImpl {
	
	public AlertStreamSchemaEntity buildTestStreamDefEntity(String programId, String streamName, String attrName) {
		AlertStreamSchemaEntity entity = new AlertStreamSchemaEntity();
		entity.setAttrType("String");
		entity.setAttrValueResolver("DefaultAttrValueResolver");
		entity.setCategory("SimpleType");
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("programId", programId);
		tags.put("streamName", streamName);
		tags.put("attrName", attrName);
		entity.setTags(tags);
		return entity;
	}
	
	@Test
	public void test() throws Exception{
        Config config = ConfigFactory.load();
		AlertStreamSchemaDAO dao = new AlertStreamSchemaDAOImpl(null, null) {
			public List<AlertStreamSchemaEntity> findAlertStreamSchemaByDataSource(String dataSource) throws Exception {
				List<AlertStreamSchemaEntity> list = new ArrayList<AlertStreamSchemaEntity>();
				String programId = "UnitTest";
				list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr1"));
				list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr2"));
				list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr3"));
				list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr4"));
				return list;
			}
		};

		StreamMetadataManager.getInstance().init(config, dao);
		Map<String, List<AlertStreamSchemaEntity>> retMap = StreamMetadataManager.getInstance().getMetadataEntitiesForAllStreams();
		Assert.assertTrue(retMap.get("TestStream").size() == 4);
		StreamMetadataManager.getInstance().reset();
	}
}
