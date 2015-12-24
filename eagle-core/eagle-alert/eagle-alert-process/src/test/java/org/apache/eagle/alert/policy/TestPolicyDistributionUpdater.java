/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.policy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAO;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.executor.AlertExecutor;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPolicyDistributionUpdater {
    private static Logger LOG = LoggerFactory.getLogger(TestPolicyDistributionUpdater.class);

    @Test
    public void testPolicyDistributionReporter() throws Exception{
        AlertDefinitionDAO alertDao = new AlertDefinitionDAOImpl(new EagleServiceConnector(null, null)) {
            @Override
            public Map<String, Map<String, AlertDefinitionAPIEntity>> findActiveAlertDefsGroupbyAlertExecutorId(String site, String dataSource) throws Exception {
                final AlertDefinitionAPIEntity entity = new AlertDefinitionAPIEntity();
                entity.setTags(new HashMap<String, String>() {{
                    put(AlertConstants.POLICY_TYPE, "siddhiCEPEngine");
                    put(AlertConstants.POLICY_ID, "policyId_1");
                }});
                Map<String, Map<String, AlertDefinitionAPIEntity>> map = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
                map.put("alertExecutorId_1", new HashMap<String, AlertDefinitionAPIEntity>() {{
                    put("policyId_1", entity);
                }});
                entity.setPolicyDef("{\"type\":\"siddhiCEPEngine\",\"expression\":\"from testStream select name insert into outputStream ;\"}");
                return map;
            }
        };

        AlertExecutor alertExecutor = new AlertExecutor("alertExecutorId_1", new DefaultPolicyPartitioner(), 1, 0, alertDao, new String[]{"testStream"}){
            public AlertStreamSchemaDAO getAlertStreamSchemaDAO(Config config){
                return new AlertStreamSchemaDAO(){
                    @Override
                    public List<AlertStreamSchemaEntity> findAlertStreamSchemaByDataSource(String dataSource) throws Exception {
                        AlertStreamSchemaEntity entity = new AlertStreamSchemaEntity();
                        entity.setTags(new HashMap<String, String>(){{
                            put("dataSource", "UnitTest");
                            put("streamName", "testStream");
                            put("attrName", "name");
                        }});
                        entity.setAttrType("string");
                        return Arrays.asList(entity);
                    }
                };
            }

            @Override
            public void report() {
                Assert.assertEquals(1, getPolicyEvaluators().size());
                LOG.info("successuflly reported");
            }
        };

        Config config = ConfigFactory.load();
        alertExecutor.prepareConfig(config);
        alertExecutor.init();
        Thread.sleep(100);
    }
}
