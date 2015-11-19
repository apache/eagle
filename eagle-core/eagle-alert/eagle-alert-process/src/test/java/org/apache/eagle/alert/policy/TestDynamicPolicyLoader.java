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
package org.apache.eagle.alert.policy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDynamicPolicyLoader {
	private final static Logger LOG = LoggerFactory.getLogger(TestDynamicPolicyLoader.class);

	@Test
	public void test() throws Exception{
		System.setProperty("config.resource", "/unittest.conf");
		Config config = ConfigFactory.load();
		Map<String, PolicyLifecycleMethods> policyChangeListeners = new HashMap<String, PolicyLifecycleMethods>();
		policyChangeListeners.put("testAlertExecutorId", new PolicyLifecycleMethods() {
			@Override
			public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
				LOG.info("deleted : " + deleted);
			}
			
			@Override
			public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
				Assert.assertTrue(added.size() == 1);
				LOG.info("added : " + added);
			}
			
			@Override
			public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
				Assert.assertTrue(changed.size() == 1);
				LOG.info("changed :" + changed);
			}
		});
		
		Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
		initialAlertDefs.put("testAlertExecutorId", new HashMap<String, AlertDefinitionAPIEntity>());
		Map<String, AlertDefinitionAPIEntity> map = initialAlertDefs.get("testAlertExecutorId");
		map.put("policyId_1", buildTestAlertDefEntity("testProgramId", "testAlertExecutorId", "policyId_1", "siddhi", "policyDef_1"));
		map.put("policyId_3", buildTestAlertDefEntity("testProgramId", "testAlertExecutorId", "policyId_3", "siddhi", "policyDef_3"));
		
		AlertDefinitionDAO dao = new AlertDefinitionDAO() {
			@Override
			public Map<String, Map<String, AlertDefinitionAPIEntity>> findActiveAlertDefsGroupbyAlertExecutorId(
					String site, String dataSource) {
				Map<String, Map<String, AlertDefinitionAPIEntity>> currentAlertDefs = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
				currentAlertDefs.put("testAlertExecutorId", new HashMap<String, AlertDefinitionAPIEntity>());
				Map<String, AlertDefinitionAPIEntity> map = currentAlertDefs.get("testAlertExecutorId");
				map.put("policyId_1", buildTestAlertDefEntity("testProgramId", "testAlertExecutorId", "policyId_1", "siddhi", "policyDef_1_1"));
				map.put("policyId_2", buildTestAlertDefEntity("testProgramId", "testAlertExecutorId", "policyId_2", "siddhi", "policyDef_2"));
				return currentAlertDefs;
			}
			
			@Override
			public List<AlertDefinitionAPIEntity> findActiveAlertDefs(String site, String dataSource) {
				return null;
			}
		};
		
		DynamicPolicyLoader loader = DynamicPolicyLoader.getInstance();
		loader.init(initialAlertDefs, dao, config);
		
		try{
			Thread.sleep(5000);
		}catch(Exception ex){
			
		}
	}
	
	public AlertDefinitionAPIEntity buildTestAlertDefEntity(String programId, String alertExecutorId, String policyId, String policyType, String policyDef) {
		AlertDefinitionAPIEntity entity = new AlertDefinitionAPIEntity();
		entity.setEnabled(true);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("programId", programId);
		tags.put("alertExecutorId", alertExecutorId);
		tags.put("policyId", policyId);
		tags.put("policyType", policyType);
		entity.setTags(tags);
		entity.setPolicyDef(policyDef);
		return entity;
	}
}
