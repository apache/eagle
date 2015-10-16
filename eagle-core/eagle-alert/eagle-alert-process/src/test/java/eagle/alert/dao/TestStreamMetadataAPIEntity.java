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
import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import eagle.alert.base.AlertTestBase;
import eagle.alert.common.AlertConstants;
import eagle.log.entity.GenericServiceAPIResponseEntity;
import eagle.service.client.IEagleServiceClient;
import eagle.service.client.impl.EagleServiceClientImpl;

public class TestStreamMetadataAPIEntity extends AlertTestBase{

	public AlertStreamSchemaEntity buildTestStreamDefEntity() {
		AlertStreamSchemaEntity entity = new AlertStreamSchemaEntity();
		entity.setAttrType("TestAttrType");
		entity.setAttrValueResolver("TestAttrValueResolver");
		entity.setCategory("TestCategory");
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("programId", "UnitTest");
		tags.put("streamName", "TestStream");
		tags.put("attrName", "TestAttrName");
		entity.setTags(tags);
		return entity;
	}
	
	@Test
	public void test() throws Exception{
		hbase.createTable("streamMetadata", "f");
		System.setProperty("config.resource", "/application.conf.3");
        Config config = ConfigFactory.load();
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort);
		
		List<AlertStreamSchemaEntity> list = new ArrayList<AlertStreamSchemaEntity>();
		list.add(buildTestStreamDefEntity());
		GenericServiceAPIResponseEntity<String> response = client.create(list);
		System.out.println(response.isSuccess());
		String query = AlertConstants.ALERT_STREAM_SCHEMA_SERVICE_ENDPOINT_NAME + "[@programId=\"" + "UnitTest" + "\"]{*}";
		GenericServiceAPIResponseEntity<AlertStreamSchemaEntity> response2 =  client.search()
																	                .startTime(0)
																	                .endTime(10 * DateUtils.MILLIS_PER_DAY)
																	                .pageSize(1000)
																	                .query(query)
																                    .send();
		List<AlertStreamSchemaEntity> ret = response2.getObj();
		Assert.assertTrue(ret.size() == 1);
		
		client.delete(list);
		response2 =  client.search()
                .startTime(0)
                .endTime(10 * DateUtils.MILLIS_PER_DAY)
                .pageSize(1000)
                .query(query)
                .send();		
		client.close();
		ret = response2.getObj();
		Assert.assertTrue(ret.size() == 0);
	}
}
