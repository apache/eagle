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
package org.apache.eagle.alert.engine.interpreter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandlers;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.exception.DefinitionNotExistException;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.*;

public class PolicyInterpreterTest {
    // -------------------------
    // Single Stream Test Cases
    // -------------------------
    @Test
    public void testParseSingleStreamPolicyQuery() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan("from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 2 min) "
            + "select cmd, user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT");
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX", executionPlan.getInputStreams().keySet().toArray()[0]);
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT", executionPlan.getOutputStreams().keySet().toArray()[0]);
        Assert.assertEquals(1, executionPlan.getStreamPartitions().size());
        Assert.assertEquals(2*60*1000,executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
    }

    @Test
    public void testParseSingleStreamPolicyWithPattern() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from e1=Stream1[price >= 20] -> e2=Stream2[price >= e1.price] \n"
                + "select e1.symbol as symbol, e2.price as price, e1.price+e2.price as total_price \n"
                + "group by symbol, company insert into OutStream");
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("Stream1"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("Stream2"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("OutStream"));
        Assert.assertEquals(StreamPartition.Type.GROUPBY,executionPlan.getStreamPartitions().get(0).getType());
        Assert.assertArrayEquals(new String[]{"symbol","company"},executionPlan.getStreamPartitions().get(0).getColumns().toArray());
        Assert.assertEquals(StreamPartition.Type.GROUPBY,executionPlan.getStreamPartitions().get(1).getType());
        Assert.assertArrayEquals(new String[]{"symbol","company"},executionPlan.getStreamPartitions().get(1).getColumns().toArray());
    }

    @Test
    public void testParseSingleStreamPolicyQueryWithMultiplePartitionUsingLargerWindow() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 1 min) "
            + "select cmd,user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1;"
            + "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 1 hour) "
            + "select cmd,user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2;"
        );
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX", executionPlan.getInputStreams().keySet().toArray()[0]);
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2"));
        Assert.assertEquals(1, executionPlan.getStreamPartitions().size());
        Assert.assertEquals(60*60*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testParseSingleStreamPolicyQueryWithConflictPartition() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 5 min) "
            + "select cmd, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1;"
            + "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 2 min) "
            + "select user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2;"
        );
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX", executionPlan.getInputStreams().keySet().toArray()[0]);
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2"));
        Assert.assertEquals(2, executionPlan.getStreamPartitions().size());
        Assert.assertEquals(5*60*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
    }

    @Test
    public void testValidPolicyWithExternalTimeWindow() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM_1#window.externalTime(timestamp, 2 min) select name, sum(value) as total group by name insert into OUTPUT_STREAM_1 ;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", mockStreamDefinition("INPUT_STREAM_2"));
                put("INPUT_STREAM_3", mockStreamDefinition("INPUT_STREAM_3"));
                put("INPUT_STREAM_4", mockStreamDefinition("INPUT_STREAM_4"));
            }
        });
        Assert.assertTrue(validation.isSuccess());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getInputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getOutputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getStreamPartitions().size());
        Assert.assertNotNull(validation.getPolicyExecutionPlan().getStreamPartitions().get(0).getSortSpec());
    }

    @Test
    public void testValidPolicyWithTimeWindow() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM_1#window.time(2 min) select name, sum(value) as total group by name insert into OUTPUT_STREAM_1 ;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
            }
        });
        Assert.assertTrue(validation.isSuccess());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getInputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getOutputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getStreamPartitions().size());
        Assert.assertNull(validation.getPolicyExecutionPlan().getStreamPartitions().get(0).getSortSpec());
    }

    @Test
    public void testValidPolicyWithTooManyInputStreams() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Arrays.asList("INPUT_STREAM_1", "INPUT_STREAM_2"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM_1[value > 90.0] select * group by name insert into OUTPUT_STREAM_1;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", mockStreamDefinition("INPUT_STREAM_2"));
            }
        });
        Assert.assertTrue(validation.isSuccess());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getInputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getOutputStreams().size());
    }

    @Test
    public void testValidPolicyWithTooFewOutputStreams() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Arrays.asList("INPUT_STREAM_1", "INPUT_STREAM_2"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue(
            "from INPUT_STREAM_1[value > 90.0] select * group by name insert into OUTPUT_STREAM_1;"
                + "from INPUT_STREAM_1[value < 90.0] select * group by name insert into OUTPUT_STREAM_2;"
        );
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", mockStreamDefinition("INPUT_STREAM_2"));
            }
        });
        Assert.assertTrue(validation.isSuccess());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getInputStreams().size());
        Assert.assertEquals(2, validation.getPolicyExecutionPlan().getOutputStreams().size());
    }

    @Test
    public void testInvalidPolicyForSyntaxError() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM (value > 90.0) select * group by name insert into OUTPUT_STREAM;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM", mockStreamDefinition("INPUT_STREAM"));
            }
        });
        Assert.assertFalse(validation.isSuccess());
    }

    @Test
    public void testInvalidPolicyForNotDefinedInputStream() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM_1[value > 90.0] select * group by name insert into OUTPUT_STREAM_1;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_2", mockStreamDefinition("INPUT_STREAM_2"));
            }
        });
        Assert.assertFalse(validation.isSuccess());
    }

    @Test
    public void testInvalidPolicyForNotDefinedOutputStream() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_2"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        definition.setValue("from INPUT_STREAM_1[value > 90.0] select * group by name insert into OUTPUT_STREAM_1;");
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
            }
        });
        Assert.assertFalse(validation.isSuccess());
    }

    // ---------------------
    // Two Stream Test Cases
    // ---------------------

    @Test
    public void testParseTwoStreamPolicyQueryWithMultiplePartition() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_1#window.externalTime(timestamp, 1 min) "
                + "select cmd,user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1;"
                + "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_2#window.externalTime(timestamp, 1 hour) "
                + "select cmd,user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2;"
        );
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_1"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_2"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2"));
        Assert.assertEquals(2, executionPlan.getStreamPartitions().size());
        Assert.assertEquals(60*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
        Assert.assertEquals(60*60*1000, executionPlan.getStreamPartitions().get(1).getSortSpec().getWindowPeriodMillis());
    }

    @Test
    public void testParseTwoStreamPolicyQueryWithSinglePartition() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_1#window.externalTime(timestamp, 1 min) "
                + "select cmd,user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1;"
                + "from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_2 select * insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2;"
        );
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_1"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_2"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_1"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT_2"));
        Assert.assertEquals(2, executionPlan.getStreamPartitions().size());
        Assert.assertEquals(60*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
        Assert.assertEquals(StreamPartition.Type.SHUFFLE, executionPlan.getStreamPartitions().get(1).getType());
    }


    @Test
    public void testParseTwoStreamPolicyQueryInnerJoin() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from TickEvent[symbol=='EBAY']#window.length(2000) " +
                "join NewsEvent#window.externalTime(timestamp, 1000 sec) \n" +
                "select * insert into JoinStream"
        );
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("TickEvent"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("NewsEvent"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("JoinStream"));
        Assert.assertEquals(StreamPartition.Type.SHUFFLE, executionPlan.getStreamPartitions().get(0).getType());
        Assert.assertNotNull(executionPlan.getStreamPartitions().get(0).getSortSpec());
        Assert.assertEquals(1000*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
        Assert.assertEquals(StreamPartition.Type.SHUFFLE, executionPlan.getStreamPartitions().get(1).getType());
        Assert.assertNull(executionPlan.getStreamPartitions().get(1).getSortSpec());
    }

    @Test
    public void testParseTwoStreamPolicyQueryInnerJoinWithCondition() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from TickEvent[symbol=='EBAY']#window.length(2000) as t unidirectional \n" +
                "join NewsEvent#window.externalTime(timestamp, 1000 sec) as n \n" +
                "on TickEvent.symbol == NewsEvent.company \n" +
                "insert into JoinStream "
        );
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("TickEvent"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("NewsEvent"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("JoinStream"));
        Assert.assertEquals(StreamPartition.Type.SHUFFLE, executionPlan.getStreamPartitions().get(0).getType());
        Assert.assertNotNull(executionPlan.getStreamPartitions().get(0).getSortSpec());
        Assert.assertEquals(1000*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
        Assert.assertEquals(StreamPartition.Type.GROUPBY, executionPlan.getStreamPartitions().get(1).getType());
        Assert.assertNull(executionPlan.getStreamPartitions().get(1).getSortSpec());
    }

    @Test
    public void testParseTwoStreamPolicyQueryInnerJoinWithConditionHavingAlias() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan(
            "from TickEvent[symbol=='EBAY']#window.length(2000) as t unidirectional \n" +
                "join NewsEvent#window.externalTime(timestamp, 1000 sec) as n \n" +
                "on t.symbol == n.company \n" +
                "insert into JoinStream "
        );
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("TickEvent"));
        Assert.assertTrue(executionPlan.getInputStreams().containsKey("NewsEvent"));
        Assert.assertTrue(executionPlan.getOutputStreams().containsKey("JoinStream"));
        Assert.assertEquals(StreamPartition.Type.SHUFFLE, executionPlan.getStreamPartitions().get(0).getType());
        Assert.assertNotNull(executionPlan.getStreamPartitions().get(0).getSortSpec());
        Assert.assertEquals(1000*1000, executionPlan.getStreamPartitions().get(0).getSortSpec().getWindowPeriodMillis());
        Assert.assertEquals(StreamPartition.Type.GROUPBY, executionPlan.getStreamPartitions().get(1).getType());
        Assert.assertNull(executionPlan.getStreamPartitions().get(1).getSortSpec());
    }

    @Test(expected = DefinitionNotExistException.class)
    public void testParseTwoStreamPolicyQueryInnerJoinWithConditionHavingNotFoundAlias() throws Exception {
        PolicyInterpreter.parseExecutionPlan(
            "from TickEvent[symbol=='EBAY']#window.length(2000) as t unidirectional \n" +
            "join NewsEvent#window.externalTime(timestamp, 1000 sec) as n \n" +
            "on t.symbol == NOT_EXIST_ALIAS.company \n" +
            "insert into JoinStream "
        );
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testLeftJoin() throws  Exception {
        PolicyDefinition def = mapper.readValue(PolicyInterpreterTest.class.getResourceAsStream("/interpreter/policy.json"), PolicyDefinition.class);
        ArrayNode array = (ArrayNode)mapper.readTree(PolicyInterpreterTest.class.getResourceAsStream("/interpreter/streams.json"));
        Map<String, StreamDefinition> allDefinitions = new HashMap<>();
        for(JsonNode node : array) {
            StreamDefinition streamDef = mapper.readValue(node.toString(), StreamDefinition.class);
            allDefinitions.put(streamDef.getStreamId(), streamDef);
        }
        PolicyValidationResult result = PolicyInterpreter.validate(def, allDefinitions);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void testExtendPolicy() throws  Exception {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test-extend-policy");
        policyDefinition.setInputStreams(Collections.singletonList("INPUT_STREAM_1"));
        policyDefinition.setOutputStreams(Collections.singletonList("OUTPUT_STREAM_1"));
        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType(PolicyStreamHandlers.CUSTOMIZED_ENGINE);
        policyDefinition.setDefinition(definition);

        Map<String, StreamDefinition> allDefinitions = new HashMap<>();
        allDefinitions.put("INPUT_STREAM_1", mockStreamDefinition("INPUT_STREAM_1"));
        PolicyValidationResult result = PolicyInterpreter.validate(policyDefinition, allDefinitions);
        Assert.assertTrue(result.isSuccess());
    }


    // --------------
    // Helper Methods
    // --------------

    private static StreamDefinition mockStreamDefinition(String streamId) {
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId(streamId);
        List<StreamColumn> columns = new ArrayList<>();
        columns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        columns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        columns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamDefinition.setColumns(columns);
        return streamDefinition;
    }

    @Test
    public void testValidPolicyWithPattern() {
        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("test_policy");
        policyDefinition.setInputStreams(Collections.singletonList("HADOOP_JMX_METRIC_STREAM_ARES"));
        policyDefinition.setOutputStreams(Collections.singletonList("HADOOP_JMX_METRIC_STREAM_ARES_MISS_BLOCKS_OUT"));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType("siddhi");
        String policy =
                "from every a = HADOOP_JMX_METRIC_STREAM_ARES[component==\"namenode\" and metric == \"hadoop.namenode.dfs.missingblocks\"] " +
                        "-> b = HADOOP_JMX_METRIC_STREAM_ARES[b.component==a.component and b.metric==a.metric and b.host==a.host and convert(b.value, \"long\") > convert(a.value, \"long\") ] " +
                        "select b.metric, b.host as host, convert(b.value, \"long\") as newNumOfMissingBlocks, convert(a.value, \"long\") as oldNumOfMissingBlocks, b.timestamp as timestamp, b.component as component, b.site as site " +
                        "group by b.metric insert into HADOOP_JMX_METRIC_STREAM_ARES_MISS_BLOCKS_OUT;";
        definition.setValue(policy);
        definition.setInputStreams(policyDefinition.getInputStreams());
        definition.setOutputStreams(policyDefinition.getOutputStreams());
        policyDefinition.setDefinition(definition);

        PolicyValidationResult validation = PolicyInterpreter.validate(policyDefinition, new HashMap<String, StreamDefinition>() {
            {
                put("HADOOP_JMX_METRIC_STREAM_ARES", mockStreamDefinition("HADOOP_JMX_METRIC_STREAM_ARES"));
            }
        });
        Assert.assertTrue(validation.isSuccess());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getInputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getOutputStreams().size());
        Assert.assertEquals(1, validation.getPolicyExecutionPlan().getStreamPartitions().size());
        Assert.assertNull(validation.getPolicyExecutionPlan().getStreamPartitions().get(0).getSortSpec());
    }
}