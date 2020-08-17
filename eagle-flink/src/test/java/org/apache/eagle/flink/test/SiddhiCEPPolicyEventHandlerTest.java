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
package org.apache.eagle.flink.test;

import org.apache.eagle.flink.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SiddhiCEPPolicyEventHandlerTest {
    private final static Logger LOG = LoggerFactory.getLogger(SiddhiCEPPolicyEventHandlerTest.class);

    private Map<String, StreamDefinition> createDefinition(String... streamIds) {
        Map<String, StreamDefinition> sds = new HashMap<>();
        for (String streamId : streamIds) {
            // construct StreamDefinition
            StreamDefinition sd = MockSampleMetadataFactory.createSampleStreamDefinition(streamId);
            sds.put(streamId, sd);
        }
        return sds;
    }

    /**
     * use Siddhi API to go through
     * 1) stream definition
     * 2) policy definition
     * 3) output collector
     */
    @Test
    public void testSimpleSiddhiPolicyEvaluation(){

    }

    @Test
    public void testBySendSimpleEvent() throws Exception {
        SiddhiPolicyHandler handler;
        MockStreamCollector collector;

        handler = new SiddhiPolicyHandler(createDefinition("sampleStream_1"), 0);
        collector = new MockStreamCollector();
        PolicyDefinition policyDefinition = MockSampleMetadataFactory.createSingleMetricSamplePolicy();
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MyStreamCounter());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evalutorId"));
        handler.prepare(context);
        StreamEvent event = StreamEvent.builder()
            .schema(MockSampleMetadataFactory.createSampleStreamDefinition("sampleStream_1"))
            .streamId("sampleStream_1")
            .timestamep(System.currentTimeMillis())
            .attributes(new HashMap<String, Object>() {{
                put("name", "cpu");
                put("value", 60.0);
                put("bad", "bad column value");
            }}).build();
        handler.send(event, collector);
        handler.close();

        Assert.assertTrue(collector.size() == 1);
    }

    @SuppressWarnings("serial")
    @Test
    public void testWithTwoStreamJoinPolicy() throws Exception {
        Map<String, StreamDefinition> ssd = createDefinition("sampleStream_1", "sampleStream_2");

        PolicyDefinition policyDefinition = new PolicyDefinition();
        policyDefinition.setName("SampleJoinPolicyForTest");
        policyDefinition.setInputStreams(Arrays.asList("sampleStream_1", "sampleStream_2"));
        policyDefinition.setOutputStreams(Collections.singletonList("joinedStream"));
        policyDefinition.setDefinition(new PolicyDefinition.Definition(PolicyStreamHandlers.SIDDHI_ENGINE,
            "from sampleStream_1#window.length(10) as left " +
                "join sampleStream_2#window.length(10) as right " +
                "on left.name == right.name and left.value == right.value " +
                "select left.timestamp,left.name,left.value " +
                "insert into joinedStream"));
        policyDefinition.setPartitionSpec(Collections.singletonList(MockSampleMetadataFactory.createSampleStreamGroupbyPartition("sampleStream_1", Collections.singletonList("name"))));
        SiddhiPolicyHandler handler;
        Semaphore mutex = new Semaphore(0);
        List<AlertStreamEvent> alerts = new ArrayList<>(0);
        Collector<AlertStreamEvent> collector = (event) -> {
            LOG.info("Collected {}", event);
            Assert.assertTrue(event != null);
            alerts.add(event);
            mutex.release();
        };

        handler = new SiddhiPolicyHandler(ssd, 0);
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MyStreamCounter());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evalutorId"));
        handler.prepare(context);


        long ts_1 = System.currentTimeMillis();
        long ts_2 = System.currentTimeMillis() + 1;

        handler.send(StreamEvent.builder()
            .schema(ssd.get("sampleStream_1"))
            .streamId("sampleStream_1")
            .timestamep(ts_1)
            .attributes(new HashMap<String, Object>() {{
                put("name", "cpu");
                put("value", 60.0);
                put("bad", "bad column value");
            }}).build(), collector);

        handler.send(StreamEvent.builder()
            .schema(ssd.get("sampleStream_2"))
            .streamId("sampleStream_2")
            .timestamep(ts_2)
            .attributes(new HashMap<String, Object>() {{
                put("name", "cpu");
                put("value", 61.0);
            }}).build(), collector);

        handler.send(StreamEvent.builder()
            .schema(ssd.get("sampleStream_2"))
            .streamId("sampleStream_2")
            .timestamep(ts_2)
            .attributes(new HashMap<String, Object>() {{
                put("name", "disk");
                put("value", 60.0);
            }}).build(), collector);

        handler.send(StreamEvent.builder()
            .schema(ssd.get("sampleStream_2"))
            .streamId("sampleStream_2")
            .timestamep(ts_2)
            .attributes(new HashMap<String, Object>() {{
                put("name", "cpu");
                put("value", 60.0);
            }}).build(), collector);

        handler.close();

        Assert.assertTrue("Should get result in 5 s", mutex.tryAcquire(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, alerts.size());
        Assert.assertEquals("joinedStream", alerts.get(0).getStreamId());
        Assert.assertEquals("cpu", alerts.get(0).getData()[1]);
    }
}