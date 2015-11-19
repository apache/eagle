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
package org.apache.eagle.alert.cep;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAO;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAOImpl;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.siddhi.EagleAlertContext;
import org.apache.eagle.alert.siddhi.SiddhiPolicyDefinition;
import org.apache.eagle.alert.siddhi.SiddhiPolicyEvaluator;
import org.apache.eagle.alert.siddhi.StreamMetadataManager;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.executor.AlertExecutor;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

public class TestSiddhiEvaluator {

	int alertCount = 0;

	public AlertStreamSchemaEntity createStreamMetaEntity(String attrName, String type) {
		AlertStreamSchemaEntity entity = new AlertStreamSchemaEntity();
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("dataSource", "hdfsAuditLog");
		tags.put("streamName", "hdfsAuditLogEventStream");
		tags.put("attrName", attrName);
		entity.setTags(tags);
		entity.setAttrType(type);
		return entity;
	}

	@Test
	public void test() throws Exception{
        Config config = ConfigFactory.load("unittest.conf");
		AlertStreamSchemaDAO streamDao = new AlertStreamSchemaDAOImpl(null, null) {
			@Override
			public List<AlertStreamSchemaEntity> findAlertStreamSchemaByDataSource(String dataSource) throws Exception {
				List<AlertStreamSchemaEntity> list = new ArrayList<AlertStreamSchemaEntity>();
				list.add(createStreamMetaEntity("cmd", "string"));
				list.add(createStreamMetaEntity("dst", "string"));
				list.add(createStreamMetaEntity("src", "string"));
				list.add(createStreamMetaEntity("host", "string"));
				list.add(createStreamMetaEntity("user", "string"));
				list.add(createStreamMetaEntity("timestamp", "long"));
				list.add(createStreamMetaEntity("securityZone", "string"));
				list.add(createStreamMetaEntity("sensitivityType", "string"));
				list.add(createStreamMetaEntity("allowed", "string"));
				return list;
			}
		};
		StreamMetadataManager.getInstance().init(config, streamDao);

		Map<String, Object> data1 =  new TreeMap<String, Object>(){{
			put("cmd", "open");
			put("dst", "");
			put("src", "");
			put("host", "");
			put("user", "");
			put("timestamp", String.valueOf(System.currentTimeMillis()));
			put("securityZone", "");
			put("sensitivityType", "");
			put("allowed", "true");
		}};
        final SiddhiPolicyDefinition policyDef = new SiddhiPolicyDefinition();
        policyDef.setType("SiddhiCEPEngine");
        String expression = "from hdfsAuditLogEventStream[cmd=='open'] " +
							"select * " +
							"insert into outputStream ;";
        policyDef.setExpression(expression);
        SiddhiPolicyEvaluator evaluator = new SiddhiPolicyEvaluator(config, "testPolicy", policyDef, new String[]{"hdfsAuditLogEventStream"});
		EagleAlertContext context = new EagleAlertContext();

		AlertDefinitionDAO alertDao = new AlertDefinitionDAOImpl(null, null) {
			@Override
			public Map<String, Map<String, AlertDefinitionAPIEntity>> findActiveAlertDefsGroupbyAlertExecutorId(String site, String dataSource) throws Exception {
				return null;
			}
		};

		context.alertExecutor = new AlertExecutor("alertExecutorId", null, 3, 1, alertDao, new String[]{"hdfsAuditLogEventStream"}) {
			@Override
			public Map<String, String> getDimensions(String policyId) {
				return new HashMap<String, String>();
			}

			@Override
			public void runMetricReporter() {}
		};
		context.alertExecutor.prepareConfig(config);
		context.alertExecutor.init();
		context.evaluator = evaluator;
		context.policyId = "testPolicy";
		context.outputCollector = new Collector<Tuple2<String, AlertAPIEntity>> () {
			@Override
			public void collect(Tuple2<String, AlertAPIEntity> stringAlertAPIEntityTuple2) {
				alertCount++;
			}
		};
		evaluator.evaluate(new ValuesArray(context, "hdfsAuditLogEventStream", data1));
		Thread.sleep(2 * 1000);
		Assert.assertEquals(alertCount, 1);
		StreamMetadataManager.getInstance().reset();
	}
}
