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
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAlertDefinitionDAOImpl {

	public AlertDefinitionAPIEntity buildTestAlertDefEntity(String site, String programId, String alertExecutorId, String policyId, String policyType) {
		AlertDefinitionAPIEntity entity = new AlertDefinitionAPIEntity();
		entity.setEnabled(true);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("site", site);
		tags.put("programId", programId);
		tags.put("alertExecutorId", alertExecutorId);
		tags.put("policyId", policyId);
		tags.put("policyType", policyType);
		entity.setTags(tags);
		return entity;
	}
	
	@Test
	public void test() throws Exception{
		Config config = ConfigFactory.load();
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		String site = "sandbox";
		String dataSource = "UnitTest";
		AlertDefinitionDAO dao = new AlertDefinitionDAOImpl(eagleServiceHost, eagleServicePort) {
			@Override
			public List<AlertDefinitionAPIEntity> findActiveAlertDefs(String site, String dataSource) throws Exception {
				List<AlertDefinitionAPIEntity> list = new ArrayList<AlertDefinitionAPIEntity>();
				list.add(buildTestAlertDefEntity(site, dataSource, "TestExecutor1", "TestPolicyIDA", "TestPolicyTypeA"));
				list.add(buildTestAlertDefEntity(site, dataSource, "TestExecutor1", "TestPolicyIDB", "TestPolicyTypeB"));
				list.add(buildTestAlertDefEntity(site, dataSource, "TestExecutor2", "TestPolicyIDC", "TestPolicyTypeC"));
				list.add(buildTestAlertDefEntity(site, dataSource, "TestExecutor2", "TestPolicyIDD", "TestPolicyTypeD"));
				return list;
			}
		};

		Map<String, Map<String, AlertDefinitionAPIEntity>> retMap = dao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
		
		Assert.assertEquals(2, retMap.size());
		Assert.assertEquals(2, retMap.get("TestExecutor1").size());
		Assert.assertEquals(2, retMap.get("TestExecutor2").size());
	}
}
