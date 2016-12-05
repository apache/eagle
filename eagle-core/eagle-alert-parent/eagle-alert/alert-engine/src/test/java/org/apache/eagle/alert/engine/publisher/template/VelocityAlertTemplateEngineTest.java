/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.publisher.template;

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.coordinator.AlertDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VelocityAlertTemplateEngineTest {
    @Test
    public void testVelocityAlertTemplate () {
        AlertTemplateEngine templateEngine = new VelocityAlertTemplateEngine();
        templateEngine.init(ConfigFactory.load());
        templateEngine.register(mockPolicy("testPolicy"));
        AlertStreamEvent event = templateEngine.filter(mockAlertEvent("testPolicy"));
        Assert.assertEquals("Alert (2016-11-30 07:31:15): cpu usage on hadoop of cluster test_cluster at localhost is 0.98, " +
            "exceeding thread hold: 90%. (policy: testPolicy, description: Policy for monitoring cpu usage > 90%), " +
            "definition: from HADOOP_JMX_METRIC_STREAM[site == \"test_cluster\" and metric == \"cpu.usage\" and value > 0.9] " +
            "select site, metric, host, role, value insert into capacityUsageAlert", event.getBody());
        Assert.assertEquals("Name Node Usage Exceed 90%, reach 98.0% now", event.getSubject());
    }

    @Test
    public void testVelocityAlertTemplateWithoutTemplate () {
        AlertTemplateEngine templateEngine = new VelocityAlertTemplateEngine();
        templateEngine.init(ConfigFactory.load());
        templateEngine.register(mockPolicyWithoutTemplate("testPolicyName"));
        AlertStreamEvent event = templateEngine.filter(mockAlertEvent("testPolicyName"));
        System.out.print(event.getBody());
        Assert.assertEquals("Message: Alert {stream=ALERT_STREAM,timestamp=2016-11-30 07:31:15,923," +
            "data={site=test_cluster, role=hadoop, metric=cpu.usage, host=localhost, value=0.98}, " +
            "policyId=testPolicyName, createdBy=junit, metaVersion=SAMPLE_META_VERSION} " +
            "(Auto-generated alert message as template not defined in policy testPolicyName)", event.getBody());
        Assert.assertEquals("testPolicyName", event.getSubject());
    }

    private static PolicyDefinition mockPolicy (String policyId) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("from HADOOP_JMX_METRIC_STREAM[site == \"test_cluster\" and metric == \"cpu.usage\" and value > 0.9] " +
            "select site, metric, host, role, value insert into capacityUsageAlert");
        def.setType("siddhi");
        pd.setDefinition(def);
        pd.setInputStreams(Collections.singletonList("HADOOP_JMX_METRIC_STREAM"));
        pd.setOutputStreams(Collections.singletonList("capacityUsageAlert"));
        pd.setName(policyId);
        pd.setDescription("Policy for monitoring cpu usage > 90%");
        AlertDefinition alertDefinition = new AlertDefinition();
        alertDefinition.setSubject("Name Node Usage Exceed 90%, reach #set($usage_per = $value * 100)$usage_per% now");
        alertDefinition.setBody("Alert ($CREATED_TIME): cpu usage on $role of cluster $site at $host is $value, exceeding thread hold: 90%. "
                + "(policy: $POLICY_ID, description: $POLICY_DESC), definition: $POLICY_DEFINITION");
        pd.setAlertDefinition(alertDefinition);
        return pd;
    }
    private static PolicyDefinition mockPolicyWithoutTemplate (String policyId) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("from HADOOP_JMX_METRIC_STREAM[site == \"test_cluster\" and metric == \"cpu.usage\" and value > 0.9] " +
            "select site, metric, host, role, value insert into capacityUsageAlert");
        def.setType("siddhi");
        pd.setDefinition(def);
        pd.setInputStreams(Collections.singletonList("HADOOP_JMX_METRIC_STREAM"));
        pd.setOutputStreams(Collections.singletonList("capacityUsageAlert"));
        pd.setName(policyId);
        pd.setDescription("Policy for monitoring cpu usage > 90%");
        return pd;
    }

    private AlertStreamEvent mockAlertEvent (String policyId) {
        AlertStreamEvent event = new AlertStreamEvent();
        event.setCreatedBy("junit");
        event.setCreatedTime(1480491075923L);
        event.setPolicyId(policyId);
        event.setStreamId("ALERT_STREAM");
        event.setSchema(mockAlertStreamDefinition("ALERT_STREAM"));
        event.setMetaVersion("SAMPLE_META_VERSION");
        event.setTimestamp(1480491075923L);
        event.setData(new Object[]{"test_cluster", "cpu.usage", "localhost", "hadoop", 0.98});
        event.ensureAlertId();
        return event;
    }

    private StreamDefinition mockAlertStreamDefinition(String streamId){
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId(streamId);
        streamDefinition.setSiteId("test_cluster");
        List<StreamColumn> columns = new ArrayList<>();
        StreamColumn column = new StreamColumn();
        column.setName("site");
        column.setType(StreamColumn.Type.STRING);
        columns.add(column);
        column = new StreamColumn();
        column.setName("metric");
        column.setType(StreamColumn.Type.STRING);
        columns.add(column);
        column = new StreamColumn();
        column.setName("host");
        column.setType(StreamColumn.Type.STRING);
        columns.add(column);
        column = new StreamColumn();
        column.setName("role");
        column.setType(StreamColumn.Type.STRING);
        columns.add(column);
        column = new StreamColumn();
        column.setName("value");
        column.setType(StreamColumn.Type.STRING);
        columns.add(column);

        streamDefinition.setColumns(columns);
        return streamDefinition;
    }
}
