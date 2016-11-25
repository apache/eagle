/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.statecheck;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.TestAlertBolt;
import org.apache.eagle.alert.engine.runner.AlertBolt;
import org.apache.eagle.alert.utils.AlertConstants;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Created on 8/4/16.
 */
public class TestStateCheckPolicy {

    @Test
    public void testStateCheck() throws Exception {
        PolicyGroupEvaluatorImpl impl = new PolicyGroupEvaluatorImpl("test-statecheck-poicyevaluator");
        AtomicBoolean verified = new AtomicBoolean(false);
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                verified.set(true);
                Assert.assertEquals("perfmon_latency_stream", ((PublishPartition) tuple.get(0)).getStreamId());
                AlertStreamEvent event = (AlertStreamEvent) tuple.get(1);
                System.out.println(String.format("collector received: [streamId=[%s], tuple=[%s] ", ((PublishPartition) tuple.get(0)).getStreamId(), tuple));
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            }

            @Override
            public void ack(Tuple input) {
            }

            @Override
            public void fail(Tuple input) {
            }

            @Override
            public void reportError(Throwable error) {
            }
        });

        AlertBolt alertBolt = TestAlertBolt.createAlertBolt(collector);
        AlertBoltSpec spec = createAlertSpec();
        Map<String, StreamDefinition> definitionMap = createStreamMap();
        
        
        List<PolicyDefinition> policies = mapper.readValue(TestStateCheckPolicy.class.getResourceAsStream("/statecheck/policies.json"),
                new TypeReference<List<PolicyDefinition>>() {
                });
        List<StreamDefinition> streams = mapper.readValue(TestStateCheckPolicy.class.getResourceAsStream("/statecheck/streamdefinitions.json"),
                new TypeReference<List<StreamDefinition>>() {
                });
        spec.addPublishPartition(streams.get(0).getStreamId(), policies.get(0).getName(), "testPublishBolt", null);
        
        alertBolt.onAlertBoltSpecChange(spec, definitionMap);

        // send data now
        sendData(alertBolt, definitionMap, spec.getBoltPoliciesMap().values().iterator().next().get(0));

        Thread.sleep(3000);
        Assert.assertTrue(verified.get());
    }

    private void sendData(AlertBolt alertBolt, Map<String, StreamDefinition> definitionMap, PolicyDefinition policyDefinition) {
        StreamDefinition definition = definitionMap.get("perfmon_latency_stream");
        long base = System.currentTimeMillis();
        for (int i = 0; i < 2; i++) {
            long time = base + i * 1000;

            Map<String, Object> mapdata = new HashMap<>();
            mapdata.put("host", "host-1");
            mapdata.put("timestamp", time);
            mapdata.put("metric", "perfmon_latency");
            mapdata.put("pool", "raptor");
            mapdata.put("value", 1000.0 + i * 1000.0);
            mapdata.put("colo", "phx");

            StreamEvent event = StreamEvent.builder().timestamep(time).attributes(mapdata, definition).build();
            PartitionedEvent pEvent = new PartitionedEvent(event, policyDefinition.getPartitionSpec().get(0), 1);

            GeneralTopologyContext mock = Mockito.mock(GeneralTopologyContext.class);

            Mockito.when(mock.getComponentId(1)).thenReturn("taskId");
            Mockito.when(mock.getComponentOutputFields("taskId", "test-stream-id")).thenReturn(new Fields(AlertConstants.FIELD_0));

            TupleImpl ti = new TupleImpl(mock, Collections.singletonList(pEvent), 1, "test-stream-id");
            alertBolt.execute(ti);
        }
    }

    @NotNull
    private Map<String, StreamDefinition> createStreamMap() throws Exception {
        List<StreamDefinition> streams = mapper.readValue(TestStateCheckPolicy.class.getResourceAsStream("/statecheck/streamdefinitions.json"),
            new TypeReference<List<StreamDefinition>>() {
            });
        return streams.stream().collect(Collectors.toMap(StreamDefinition::getStreamId, item -> item));
    }

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private AlertBoltSpec createAlertSpec() throws Exception {
        AlertBoltSpec spec = new AlertBoltSpec();

        spec.setVersion("version1");
        spec.setTopologyName("testTopology");

        List<PolicyDefinition> policies = mapper.readValue(TestStateCheckPolicy.class.getResourceAsStream("/statecheck/policies.json"),
            new TypeReference<List<PolicyDefinition>>() {
            });
        Assert.assertTrue(policies.size() > 0);
        spec.addBoltPolicy("alertBolt1", policies.get(0).getName());
        spec.getBoltPoliciesMap().put("alertBolt1", new ArrayList<>(policies));

        return spec;
    }

}
