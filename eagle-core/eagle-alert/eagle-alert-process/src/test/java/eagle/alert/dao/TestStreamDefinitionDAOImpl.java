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
package eagle.alert.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eagle.alert.entity.AlertStreamSchemaEntity;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eagle.common.config.EagleConfigConstants;
import org.junit.Assert;
import org.junit.Test;

import eagle.alert.base.AlertTestBase;
import eagle.alert.siddhi.StreamMetadataManager;
import eagle.service.client.IEagleServiceClient;
import eagle.service.client.impl.EagleServiceClientImpl;

public class TestStreamDefinitionDAOImpl extends AlertTestBase{
	
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
		hbase.createTable("streamMetadata", "f");
		System.setProperty("config.resource", "/application.conf");

        Config config = ConfigFactory.load();
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		AlertStreamSchemaDAO dao = new AlertStreamSchemaDAOImpl(eagleServiceHost, eagleServicePort);
		
		List<AlertStreamSchemaEntity> list = new ArrayList<AlertStreamSchemaEntity>();
		String programId = "UnitTest";
		list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr1"));
		list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr2"));
		list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr3"));
		list.add(buildTestStreamDefEntity(programId, "TestStream", "Attr4"));
		IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort);
		client.create(list);
		
		Thread.sleep(5000);

		StreamMetadataManager.getInstance().init(config, dao);
		Map<String, List<AlertStreamSchemaEntity>> retMap = StreamMetadataManager.getInstance().getMetadataEntitiesForAllStreams();
		Assert.assertTrue(retMap.get("TestStream").size() == 4);
		
		client.delete(list);
		client.close();
	}
}
