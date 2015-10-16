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
package eagle.alert.cep;

import java.util.Map;
import java.util.TreeMap;

import eagle.alert.dao.AlertStreamSchemaDAO;
import eagle.alert.dao.AlertStreamSchemaDAOImpl;
import eagle.alert.siddhi.SiddhiPolicyEvaluator;
import eagle.alert.siddhi.StreamMetadataManager;
import eagle.common.config.EagleConfigConstants;
import org.junit.Test;

import eagle.alert.siddhi.SiddhiPolicyDefinition;
import eagle.dataproc.core.ValuesArray;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestSiddhiEvaluator {//extends AlertTestBase {
	
	@Test
	public void test() throws Exception{
		//hbase.createTable("streamMetadata", "f");
        Config config = ConfigFactory.load("unittest.conf");
		                       
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
		AlertStreamSchemaDAO dao = new AlertStreamSchemaDAOImpl(eagleServiceHost, eagleServicePort);
		StreamMetadataManager.getInstance().init(config, dao);
		
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
		
        SiddhiPolicyDefinition policyDef = new SiddhiPolicyDefinition();
        policyDef.setType("SiddhiCEPEngine");
        String expression = "from hdfsAuditLogEventStream[cmd=='open'] " +
                "select * " +
                "insert into outputStream ;";
        policyDef.setExpression(expression);        
        SiddhiPolicyEvaluator evaluator = new SiddhiPolicyEvaluator(config, "testPolicy", policyDef, new String[]{"hdfsAuditLogEventStream"});
		
		evaluator.evaluate(new ValuesArray("hdfsAuditLogEventStream", data1));		
		
//		List<AlertAPIEntity> list = evaluator.fetchAlerts();
//		Assert.assertTrue(list.size() == 1);
//		for (AlertAPIEntity entity : list) {
//			System.out.println(entity.getAlertContext().toString());
//		}	
	}
}
