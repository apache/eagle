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
package org.apache.eagle.alert.executor;

import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.config.AbstractPolicyDefinition;
import org.apache.eagle.policy.dao.AlertStreamSchemaDAOImpl;
import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.policy.PolicyEvaluator;
import org.apache.eagle.policy.PolicyManager;
import org.apache.eagle.policy.siddhi.SiddhiPolicyDefinition;
import org.apache.eagle.policy.siddhi.StreamMetadataManager;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import junit.framework.Assert;

/**
 * @since Dec 18, 2015
 *
 */
public class TestPolicyExecutor {

	public static class T2 extends AbstractPolicyDefinitionEntity {
		@Override
		public String getPolicyDef() {
			return null;
		}
		@Override
		public boolean isEnabled() {
			return false;
		}
	}

	// not feasible to Unit test, it requires the local service.
	@Ignore
	@Test
	public void testReflectCreatePolicyEvaluator() throws Exception {
		System.setProperty("config.resource", "/unittest.conf");
		String policyType = Constants.policyType.siddhiCEPEngine.name();
		Class<? extends PolicyEvaluator> evalCls = PolicyManager.getInstance().getPolicyEvaluator(policyType);
		Config config = ConfigFactory.load();

		String def = "{\"expression\":\"from hdfsAuditLogEventStream select * insert into outputStream;\",\"type\":\"siddhiCEPEngine\"}";
		// test1 : test json deserialization
		AbstractPolicyDefinition policyDef = null;
		policyDef = JsonSerDeserUtils.deserialize(def, AbstractPolicyDefinition.class,
				PolicyManager.getInstance().getPolicyModules(policyType));
		// Assert conversion succeed
		Assert.assertEquals(SiddhiPolicyDefinition.class, policyDef.getClass());

		// make sure meta data manager initialized
		StreamMetadataManager.getInstance().init(config, new AlertStreamSchemaDAOImpl(config));

		String[] sourceStreams = new String[] { "hdfsAuditLogEventStream" };
		// test2 : test evaluator
		PolicyEvaluator pe = evalCls.getConstructor(Config.class, String.class, AbstractPolicyDefinition.class,
				String[].class, boolean.class).newInstance(config, "policy-id", policyDef, sourceStreams, false);

		PolicyEvaluator<AlertDefinitionAPIEntity> e1 = (PolicyEvaluator<AlertDefinitionAPIEntity>) pe;

		PolicyEvaluator<T2> e2 = (PolicyEvaluator<T2>) pe;

	}

}
