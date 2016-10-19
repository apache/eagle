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
package org.apache.eagle.service.metadata.resource;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PolicyInterpreterTest {
    @Test
    public void parseFullPolicyQuery() throws Exception {
        PolicyExecutionPlan executionPlan = PolicyInterpreter.parseExecutionPlan("from HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX#window.externalTime(timestamp, 2 min) "
            + "select cmd, user, count() as total_count group by cmd,user insert into HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT");
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX", executionPlan.getInputStreams().keySet().toArray()[0]);
        Assert.assertEquals("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX_OUT", executionPlan.getOutputStreams().keySet().toArray()[0]);
        Assert.assertEquals(1, executionPlan.getStreamPartitions().size());
        Assert.assertNotNull(executionPlan.getStreamPartitions().get(0).getSortSpec());
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
                put("INPUT_STREAM_1", createStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", createStreamDefinition("INPUT_STREAM_2"));
                put("INPUT_STREAM_3", createStreamDefinition("INPUT_STREAM_3"));
                put("INPUT_STREAM_4", createStreamDefinition("INPUT_STREAM_4"));
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
                put("INPUT_STREAM_1", createStreamDefinition("INPUT_STREAM_1"));
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
                put("INPUT_STREAM_1", createStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", createStreamDefinition("INPUT_STREAM_2"));
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
                put("INPUT_STREAM_1", createStreamDefinition("INPUT_STREAM_1"));
                put("INPUT_STREAM_2", createStreamDefinition("INPUT_STREAM_2"));
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
                put("INPUT_STREAM", createStreamDefinition("INPUT_STREAM"));
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
                put("INPUT_STREAM_2", createStreamDefinition("INPUT_STREAM_2"));
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
                put("INPUT_STREAM_1", createStreamDefinition("INPUT_STREAM_1"));
            }
        });
        Assert.assertFalse(validation.isSuccess());
    }

    // --------------
    // Helper Methods
    // --------------

    private static StreamDefinition createStreamDefinition(String streamId) {
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId(streamId);
        List<StreamColumn> columns = new ArrayList<>();
        columns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        columns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        columns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamDefinition.setColumns(columns);
        return streamDefinition;
    }
}