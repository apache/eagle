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
package org.apache.eagle.alert.engine.router;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.evaluator.PolicyGroupEvaluator;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandlers;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.runner.AlertBolt;
import org.apache.eagle.alert.engine.runner.TestStreamRouterBolt;
import org.apache.eagle.alert.engine.serialization.impl.PartitionedEventSerializerImpl;
import org.apache.eagle.common.DateTimeUtil;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Since 5/2/16.
 */
@SuppressWarnings( {"rawtypes", "unused"})
public class TestAlertBolt {

    public static final String TEST_STREAM = "test-stream";

    /**
     * Following knowledge is guaranteed in
     *
     * @throws Exception Add test case: 2 alerts should be generated even if they are very close to each other in timestamp
     * @see org.apache.eagle.alert.engine.runner.AlertBolt#execute{
     * if(!routedStreamEvent.getRoute().getTargetComponentId().equals(this.policyGroupEvaluator.getName())){
     * throw new IllegalStateException("Got event targeted to "+ routedStreamEvent.getRoute().getTargetComponentId()+" in "+this.policyGroupEvaluator.getName());
     * }
     * }
     */
    @Test
    public void testAlertBolt() throws Exception {
        final AtomicInteger alertCount = new AtomicInteger();
        final Semaphore mutex = new Semaphore(0);
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            int count = 0;

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                alertCount.incrementAndGet();
                mutex.release();
                Assert.assertEquals("testAlertStream", tuple.get(0));
                AlertStreamEvent event = (AlertStreamEvent) tuple.get(1);
                System.out.println(String.format("collector received: [streamId=[%s], tuple=[%s] ", streamId, tuple));
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
        AlertBolt bolt = createAlertBolt(collector);

        String streamId = "cpuUsageStream";

        // construct StreamDefinition
        StreamDefinition schema = new StreamDefinition();
        schema.setStreamId(streamId);
        StreamColumn column = new StreamColumn();
        column.setName("col1");
        column.setType(StreamColumn.Type.STRING);
        schema.setColumns(Collections.singletonList(column));
        Map<String, StreamDefinition> sds = new HashMap<>();
        sds.put(schema.getStreamId(), schema);

        // construct StreamPartition
        StreamPartition sp = new StreamPartition();
        sp.setColumns(Collections.singletonList("col1"));
        sp.setStreamId(streamId);
        sp.setType(StreamPartition.Type.GROUPBY);

        AlertBoltSpec spec = new AlertBoltSpec();
        spec.setVersion("version1");
        spec.setTopologyName("testTopology");
        PolicyDefinition pd = new PolicyDefinition();
        pd.setName("policy1");
        pd.setPartitionSpec(Collections.singletonList(sp));
        pd.setOutputStreams(Collections.singletonList("testAlertStream"));
        pd.setInputStreams(Collections.singletonList(streamId));
        pd.setDefinition(new PolicyDefinition.Definition());
        pd.getDefinition().type = PolicyStreamHandlers.SIDDHI_ENGINE;
        pd.getDefinition().value = "from cpuUsageStream[col1=='value1' OR col1=='value2'] select col1 insert into testAlertStream;";
        spec.addBoltPolicy("alertBolt1", pd.getName());
        spec.getBoltPoliciesMap().put("alertBolt1", new ArrayList<PolicyDefinition>(Arrays.asList(pd)));
        bolt.onAlertBoltSpecChange(spec, sds);

        // contruct GeneralTopologyContext
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        int taskId = 1;
        when(context.getComponentId(taskId)).thenReturn("comp1");
        when(context.getComponentOutputFields("comp1", "default")).thenReturn(new Fields("f0"));

        // construct event with "value1"
        StreamEvent event1 = new StreamEvent();
        event1.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:00:00") * 1000);
        event1.setMetaVersion("version1");
        Object[] data = new Object[] {"value1"};
        event1.setData(data);
        event1.setStreamId(streamId);
        PartitionedEvent partitionedEvent1 = new PartitionedEvent(event1, sp, 1001);

        // construct another event with "value1"
        StreamEvent event2 = new StreamEvent();
        event2.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:00:00") * 1000);
        event2.setMetaVersion("version1");
        data = new Object[] {"value2"};
        event2.setData(data);
        event2.setStreamId(streamId);
        PartitionedEvent partitionedEvent2 = new PartitionedEvent(event2, sp, 1001);

        Tuple input = new TupleImpl(context, Collections.singletonList(partitionedEvent1), taskId, "default");
        Tuple input2 = new TupleImpl(context, Collections.singletonList(partitionedEvent2), taskId, "default");
        bolt.execute(input);
        bolt.execute(input2);
        Assert.assertTrue("Timeout to acquire mutex in 5s", mutex.tryAcquire(2, 5, TimeUnit.SECONDS));
        Assert.assertEquals(2, alertCount.get());
        bolt.cleanup();
    }

    public static AlertBolt createAlertBolt(OutputCollector collector) {
        Config config = ConfigFactory.load();
        PolicyGroupEvaluator policyGroupEvaluator = new PolicyGroupEvaluatorImpl("testPolicyGroupEvaluatorImpl");
        TestStreamRouterBolt.MockChangeService mockChangeService = new TestStreamRouterBolt.MockChangeService();
        AlertBolt bolt = new AlertBolt("alertBolt1", config, mockChangeService);
        Map stormConf = new HashMap<>();
        TopologyContext topologyContext = mock(TopologyContext.class);
        when(topologyContext.registerMetric(any(String.class), any(MultiCountMetric.class), any(int.class))).thenReturn(new MultiCountMetric());
        bolt.prepare(stormConf, topologyContext, collector);
        return bolt;
    }

    @Test
    public void testMetadataMismatch() throws Exception {
        AtomicInteger failedCount = new AtomicInteger();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            int count = 0;

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                Assert.assertEquals("testAlertStream", tuple.get(0));
                AlertStreamEvent event = (AlertStreamEvent) tuple.get(1);
                System.out.println(String.format("collector received: [streamId=[%s], tuple=[%s] ", streamId, tuple));
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
                failedCount.incrementAndGet();
            }

            @Override
            public void reportError(Throwable error) {
            }
        });
        AlertBolt bolt = createAlertBolt(collector);

        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        int taskId = 1;
        when(context.getComponentId(taskId)).thenReturn("comp1");
        when(context.getComponentOutputFields("comp1", "default")).thenReturn(new Fields("f0"));
        // case 1: bolt prepared but metadata not initialized (no bolt.onAlertBoltSpecChange)
        PartitionedEvent pe = new PartitionedEvent();
        pe.setPartitionKey(1);
        pe.setPartition(createPartition());
        StreamEvent streamEvent = new StreamEvent();
        streamEvent.setStreamId("test-stream");
        streamEvent.setTimestamp(System.currentTimeMillis());
        pe.setEvent(streamEvent);

        PartitionedEventSerializerImpl peSer = new PartitionedEventSerializerImpl(bolt);
        byte[] serializedEvent = peSer.serialize(pe);
        Tuple input = new TupleImpl(context, Collections.singletonList(serializedEvent), taskId, "default");
        bolt.execute(input);

        Assert.assertEquals(1, failedCount.get());
        failedCount.set(0);

        {
            // case 2: metadata loaded but empty (AlertBoltSepc)
            bolt.onAlertBoltSpecChange(new AlertBoltSpec(), new HashMap());

            bolt.execute(input);
            Assert.assertEquals(1, failedCount.get());
            failedCount.set(0);
        }

        // case 3: metadata loaded but mismatched
        {
            Map<String, StreamDefinition> sds = new HashMap();
            StreamDefinition sdTest = new StreamDefinition();
            String streamId = "pd-test"; // here streamId is different from the one "test-stream" (StreamEvent)
            sdTest.setStreamId(streamId);
            sds.put(sdTest.getStreamId(), sdTest);

            AlertBoltSpec boltSpecs = new AlertBoltSpec();
            boltSpecs.setVersion("specVersion-" + System.currentTimeMillis());

            PolicyDefinition def = new PolicyDefinition();
            def.setName("policy-definition");
            def.setInputStreams(Arrays.asList(streamId));
            def.setOutputStreams(Arrays.asList("output"));
            PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
            definition.setType(PolicyStreamHandlers.NO_DATA_ALERT_ENGINE);
            definition.setValue("PT0M,provided,1,host,host1");
            def.setDefinition(definition);

            boltSpecs.getBoltPoliciesMap().put(bolt.getBoltId(), Arrays.asList(def));

            bolt.onAlertBoltSpecChange(boltSpecs, sds);

            bolt.execute(input);
            Assert.assertEquals(1, failedCount.get());
            failedCount.set(0);
        }
    }

    //TODO: no data alert failed, need to check when no data alert merged.
    @Test
    public void testMetaversionConflict() throws Exception {
        AtomicInteger failedCount = new AtomicInteger();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            int count = 0;

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                Assert.assertEquals("testAlertStream", tuple.get(0));
                AlertStreamEvent event = (AlertStreamEvent) tuple.get(1);
                System.out.println(String.format("collector received: [streamId=[%s], tuple=[%s] ", streamId, tuple));
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
                failedCount.incrementAndGet();
            }

            @Override
            public void reportError(Throwable error) {
            }
        });
        AlertBolt bolt = createAlertBolt(collector);

        Map<String, StreamDefinition> sds = new HashMap();
        StreamDefinition sdTest = new StreamDefinition();
        String streamId = "test-stream";
        sdTest.setStreamId(streamId);
        sds.put(sdTest.getStreamId(), sdTest);

        AlertBoltSpec boltSpecs = new AlertBoltSpec();
        boltSpecs.setVersion("spec_version_" + System.currentTimeMillis());
        boltSpecs.setTopologyName("alertUnitTopology_1");

        PolicyDefinition def = new PolicyDefinition();
        def.setName("policy-definition");
        def.setInputStreams(Arrays.asList(streamId));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType(PolicyStreamHandlers.NO_DATA_ALERT_ENGINE);
        definition.setValue("PT0M,provided,1,host,host1");
        def.setDefinition(definition);
        def.setPartitionSpec(Arrays.asList(createPartition()));
        def.setOutputStreams(Arrays.asList("out"));

        boltSpecs.getBoltPoliciesMap().put(bolt.getBoltId(), Arrays.asList(def));
        bolt = createAlertBolt(collector);
        bolt.onAlertBoltSpecChange(boltSpecs, sds);

        Tuple input = createTuple(bolt, boltSpecs.getVersion());
        bolt.execute(input);

        // Sleep 10s to wait thread in bolt.execute() to finish works
        Thread.sleep(10000);

        Assert.assertEquals(0, failedCount.get());
        failedCount.set(0);

    }

    private Tuple createTuple(AlertBolt bolt, String version) throws IOException {
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        int taskId = 1;
        when(context.getComponentId(taskId)).thenReturn("comp1");
        when(context.getComponentOutputFields("comp1", "default")).thenReturn(new Fields("f0"));
        // case 1: bolt prepared but metadata not initialized (no bolt.onAlertBoltSpecChange)
        PartitionedEvent pe = new PartitionedEvent();
        pe.setPartitionKey(1);
        pe.setPartition(createPartition());
        StreamEvent streamEvent = new StreamEvent();
        streamEvent.setStreamId(TEST_STREAM);
        streamEvent.setTimestamp(System.currentTimeMillis());
        streamEvent.setMetaVersion(version);
        pe.setEvent(streamEvent);

        PartitionedEventSerializerImpl peSer = new PartitionedEventSerializerImpl(bolt);
        byte[] serializedEvent = peSer.serialize(pe);
        return new TupleImpl(context, Collections.singletonList(serializedEvent), taskId, "default");
    }

    private StreamPartition createPartition() {
        StreamPartition sp = new StreamPartition();
        sp.setStreamId(TEST_STREAM);
        sp.setType(StreamPartition.Type.GROUPBY);
        return sp;
    }

    @Test
    public void testExtendDefinition() throws IOException {
        PolicyDefinition def = new PolicyDefinition();
        def.setName("policy-definition");
        def.setInputStreams(Arrays.asList(TEST_STREAM));

        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        definition.setType(PolicyStreamHandlers.CUSTOMIZED_ENGINE);
        definition.setHandlerClass("org.apache.eagle.alert.engine.router.CustomizedHandler");
        definition.setValue("PT0M,plain,1,host,host1");
        def.setDefinition(definition);
        def.setPartitionSpec(Arrays.asList(createPartition()));

        AlertBoltSpec boltSpecs = new AlertBoltSpec();

        AtomicBoolean recieved = new AtomicBoolean(false);
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                recieved.set(true);
                return Collections.emptyList();
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
        AlertBolt bolt = createAlertBolt(collector);

        boltSpecs.getBoltPoliciesMap().put(bolt.getBoltId(), Arrays.asList(def));
        boltSpecs.setVersion("spec_" + System.currentTimeMillis());
        // stream def map
        Map<String, StreamDefinition> sds = new HashMap();
        StreamDefinition sdTest = new StreamDefinition();
        sdTest.setStreamId(TEST_STREAM);
        sds.put(sdTest.getStreamId(), sdTest);

        bolt.onAlertBoltSpecChange(boltSpecs, sds);

        // how to assert
        Tuple t = createTuple(bolt, boltSpecs.getVersion());

        bolt.execute(t);

        Assert.assertTrue(recieved.get());
    }

    @Test @Ignore
    public void testMultiStreamDefinition() throws Exception {
        final AtomicInteger alertCount = new AtomicInteger();
        final Semaphore mutex = new Semaphore(0);
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            int count = 0;

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                System.out.println("=====output collector==========");
                alertCount.incrementAndGet();
                mutex.release();
                Assert.assertTrue("symptomaticAlertOutputStream".equals((String) tuple.get(0))
                    || "deviceDownAlertStream".equals((String) tuple.get(0)));
                AlertStreamEvent event = (AlertStreamEvent) tuple.get(1);
                System.out.println(String.format("collector received: [streamId=[%s], tuple=[%s] ", streamId, tuple));

                System.out.println("**********output collector end***********");
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


        AlertBolt bolt = createAlertBolt(collector);

        // construct StreamPartition
        StreamPartition sp = new StreamPartition();
        sp.setColumns(Collections.singletonList("col1"));
        sp.setStreamId("correlatedStream");
        sp.setType(StreamPartition.Type.GROUPBY);

        pushAlertBoltSpec(sp, bolt);

        // now emit
        // contruct GeneralTopologyContext
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        int taskId = 1;
        when(context.getComponentId(taskId)).thenReturn("comp1");
        when(context.getComponentOutputFields("comp1", "default")).thenReturn(new Fields("f0"));

        long base = System.currentTimeMillis();
        int i = 0;
        String linkedSwitch = "lvs-ra-01";

        // construct event with "value1"
        StreamEvent event1 = new StreamEvent();
        event1.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:00:00") * 1000);
        event1.setMetaVersion("version1");
        Object[] data = new Object[] { base , "child-"+ (i++), "", linkedSwitch};
        event1.setData(data);
        event1.setStreamId("correlatedStream");
        PartitionedEvent partitionedEvent1 = new PartitionedEvent(event1, sp, 1001);

        // construct another event with "value1"
        StreamEvent event2 = new StreamEvent();
        event2.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:05:00") * 1000);
        event2.setMetaVersion("version1");
        data = new Object[] { base , "child-"+ (i++), "", linkedSwitch};
        event2.setData(data);
        event2.setStreamId("correlatedStream");
        PartitionedEvent partitionedEvent2 = new PartitionedEvent(event2, sp, 1001);

        Tuple input = new TupleImpl(context, Collections.singletonList(partitionedEvent1), taskId, "default");
        Tuple input2 = new TupleImpl(context, Collections.singletonList(partitionedEvent2), taskId, "default");
        bolt.execute(input);
        bolt.execute(input2);
        Assert.assertTrue("Timeout to acquire mutex in 10s", mutex.tryAcquire(1, 10, TimeUnit.SECONDS));
        Assert.assertEquals(3, alertCount.get());
        bolt.cleanup();
    }

    private void pushAlertBoltSpec(StreamPartition sp, AlertBolt bolt) {
        Map<String, StreamDefinition> sds = new HashMap<>();
        sds.put("correlatedStream", createCorrelateStream("correlatedStream"));
        sds.put("symptomaticAlertOutputStream", createCorrelateStream("symptomaticAlertOutputStream")); // output of updated correlatedStream
        sds.put("deviceDownAlertStream", createCorrelateStream("deviceDownAlertStream"));

        PolicyDefinition pd = new PolicyDefinition();
        pd.setName("network_symptomatic");
        pd.setInputStreams(Arrays.asList("correlatedStream"));
        pd.setOutputStreams(Arrays.asList("deviceDownAlertStream", "symptomaticAlertOutputStream"));

        pd.setPartitionSpec(Arrays.asList(sp));

        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setType(PolicyStreamHandlers.SIDDHI_ENGINE);
        def.setValue("from correlatedStream#window.externalTime(timestamp, 3 min) select UUID() as docId, linkedSwitch, '' as parentKey, timestamp group by linkedSwitch having count() > 0 insert into deviceDownAlertStream; " +
            " from correlatedStream#window.externalTime(timestamp, 3 min) as left join deviceDownAlertStream#window.time(3 min) as right on left.linkedSwitch == right.linkedSwitch" +
            " select left.docId, left.timestamp, left.linkedSwitch, right.docId as parentKey insert into symptomaticAlertOutputStream;");
        pd.setDefinition(def);


        AlertBoltSpec spec = new AlertBoltSpec();
        spec.setVersion("version1");
        spec.setTopologyName("testTopology");
        spec.addBoltPolicy("alertBolt1", pd.getName());
        spec.getBoltPoliciesMap().put("alertBolt1", new ArrayList<>(Arrays.asList(pd)));

        bolt.onAlertBoltSpecChange(spec, sds);
    }

    private StreamDefinition createCorrelateStream(String streamId) {
        // construct StreamDefinition
        StreamDefinition schema = new StreamDefinition();
        schema.setStreamId(streamId);
        List<StreamColumn> columns = new LinkedList<>();
        {
            StreamColumn column = new StreamColumn();
            column.setName("timestamp");
            column.setType(StreamColumn.Type.LONG);
            columns.add(column);
        }
        {
            StreamColumn column = new StreamColumn();
            column.setName("docId");
            column.setType(StreamColumn.Type.STRING);
            columns.add(column);
        }
        {
            StreamColumn column = new StreamColumn();
            column.setName("parentKey");
            column.setType(StreamColumn.Type.STRING);
            columns.add(column);
        }
        {
            StreamColumn column = new StreamColumn();
            column.setName("linkedSwitch");
            column.setType(StreamColumn.Type.STRING);
            columns.add(column);
        }

        schema.setColumns(columns);
        return schema;
    }

}

