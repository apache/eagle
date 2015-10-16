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

import eagle.common.config.EagleConfigConstants;
import org.junit.Assert;
import org.junit.Test;

import eagle.alert.base.AlertTestBase;
import eagle.alert.entity.AlertDefinitionAPIEntity;
import eagle.service.client.IEagleServiceClient;
import eagle.service.client.impl.EagleServiceClientImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestAlertDefinitionDAOImpl extends AlertTestBase{

	public AlertDefinitionAPIEntity buildTestAlertDefEntity(String programId, String alertExecutorId, String policyId, String policyType) {
		AlertDefinitionAPIEntity entity = new AlertDefinitionAPIEntity();
		entity.setEnabled(true);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("programId", programId);
		tags.put("alertExecutorId", alertExecutorId);
		tags.put("policyId", policyId);
		tags.put("policyType", policyType);
		entity.setTags(tags);
		return entity;
	}
	
	@Test
	public void test() throws Exception{
		hbase.createTable("alertdef", "f");
		System.setProperty("config.resource", "/application.conf");
		Config config = ConfigFactory.load();
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		List<AlertDefinitionAPIEntity> list = new ArrayList<AlertDefinitionAPIEntity>();
        String site = "sandbox";
		String dataSource = "UnitTest";
		list.add(buildTestAlertDefEntity(dataSource, "TestExecutor1", "TestPolicyIDA", "TestPolicyTypeA"));
		list.add(buildTestAlertDefEntity(dataSource, "TestExecutor1", "TestPolicyIDB", "TestPolicyTypeB"));
		list.add(buildTestAlertDefEntity(dataSource, "TestExecutor2", "TestPolicyIDC", "TestPolicyTypeC"));
		list.add(buildTestAlertDefEntity(dataSource, "TestExecutor2", "TestPolicyIDD", "TestPolicyTypeD"));
		IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort);
		client.create(list);
		
		AlertDefinitionDAO dao = new AlertDefinitionDAOImpl(eagleServiceHost, eagleServicePort);
		dao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
				
		Map<String, Map<String, AlertDefinitionAPIEntity>> retMap = dao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
		
		Assert.assertEquals(2, retMap.size());
		Assert.assertEquals(2, retMap.get("TestExecutor1").size());
		Assert.assertEquals(2, retMap.get("TestExecutor2").size());
		
		client.delete(list);
		client.close();
	}
}
