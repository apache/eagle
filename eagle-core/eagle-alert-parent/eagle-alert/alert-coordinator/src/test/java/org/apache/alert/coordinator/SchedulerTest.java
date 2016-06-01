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
package org.apache.alert.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.alert.coordinator.mock.TestTopologyMgmtService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.StreamRepartitionStrategy;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamWorkSlotQueue;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.ScheduleOption;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.impl.GreedyPolicyScheduler;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.coordinator.provider.InMemScheduleConext;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition.Definition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamColumn.Type;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.ConfigFactory;

/**
 * @since Apr 22, 2016
 *
 */
public class SchedulerTest {

    private static final String STREAM2 = "stream2";
    private static final String JOIN_POLICY_1 = "join-policy-1";
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_POLICY_1 = "test-policy1";
    private static final String TEST_POLICY_2 = "test-policy2";
    private static final String TEST_POLICY_3 = "test-policy3";
    private static final String STREAM1 = "stream1";
    private static final String DS_NAME = "ds1";
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerTest.class);

    @BeforeClass
    public static void setup() {
        ConfigFactory.invalidateCaches();
        System.setProperty("config.resource", "/application.conf");
    }
    
    @Test
    public void test01_simple() throws Exception {
        GreedyPolicyScheduler ps = new GreedyPolicyScheduler();
        IScheduleContext context = createScheduleContext();
        ps.init(context, createMgmtService());
        ps.schedule(new ScheduleOption());

        ScheduleState status = ps.getState();
        context = ps.getContext(); // context updated!
        Map<String, SpoutSpec> spec = status.getSpoutSpecs();

        LOG.info(mapper.writeValueAsString(spec));
        Assert.assertEquals(2, spec.size());
        Assert.assertTrue(spec.containsKey("topo1"));
        assertFirstPolicyScheduled(context, status);
    }

    private void assertFirstPolicyScheduled(IScheduleContext context, ScheduleState status) {
        String version = status.getVersion();
        // assert spout spec
        {
            Iterator<SpoutSpec> it = status.getSpoutSpecs().values().iterator();
            {
                // assert spout 1
                SpoutSpec ss = it.next();
                Assert.assertEquals(version, ss.getVersion());
                Assert.assertEquals(1, ss.getKafka2TupleMetadataMap().size());
                Assert.assertEquals(TEST_TOPIC, ss.getKafka2TupleMetadataMap().keySet().iterator().next());

                Assert.assertEquals(1, ss.getStreamRepartitionMetadataMap().size());
                List<StreamRepartitionMetadata> metas = ss.getStreamRepartitionMetadataMap().values().iterator().next();
                Assert.assertEquals(1, metas.size());

                StreamRepartitionMetadata streamMeta = metas.iterator().next();
                Assert.assertEquals(STREAM1, streamMeta.getStreamId());
                Assert.assertEquals(DS_NAME, streamMeta.getTopicName());
                Assert.assertEquals(1, streamMeta.groupingStrategies.size());

                StreamRepartitionStrategy gs = streamMeta.groupingStrategies.iterator().next();
                Assert.assertEquals(5, gs.numTotalParticipatingRouterBolts);
                Assert.assertEquals(5, gs.totalTargetBoltIds.size());
                Assert.assertEquals(0, gs.startSequence);

                Assert.assertTrue(context.getPolicyAssignments().containsKey(TEST_POLICY_1));
            }
            {
                // assert spout 2
                SpoutSpec ss = it.next();
                Assert.assertEquals(version, ss.getVersion());
                Assert.assertEquals(0, ss.getKafka2TupleMetadataMap().size());
            }
        }
        // assert grp-by spec
        {
            Iterator<RouterSpec> gsit = status.getGroupSpecs().values().iterator();
            {
                Assert.assertEquals(2, status.getGroupSpecs().values().size());
                Assert.assertTrue(gsit.hasNext());
                RouterSpec gspec = gsit.next();
                Assert.assertEquals(version, gspec.getVersion());
                String topo1 = gspec.getTopologyName();
                LOG.info("group spec topology name:", topo1);
                List<StreamRouterSpec> routeSpecs = gspec.getRouterSpecs();
                Assert.assertEquals(1, routeSpecs.size());
                for (StreamRouterSpec spec : routeSpecs) {
                    StreamPartition par = spec.getPartition();
                    Assert.assertEquals(STREAM1, par.getStreamId());
                    Assert.assertEquals(Arrays.asList("col1"), par.getColumns());
                    Assert.assertEquals(STREAM1, spec.getStreamId());

                    Assert.assertEquals(1, spec.getTargetQueue().size());
                    List<PolicyWorkerQueue> queues = spec.getTargetQueue();
                    Assert.assertEquals(1, queues.size());
                    Assert.assertEquals(5, queues.get(0).getWorkers().size());
                    for (WorkSlot slot : queues.get(0).getWorkers()) {
                        Assert.assertEquals(topo1, slot.getTopologyName());
                        LOG.info(slot.getBoltId());
                    }
                }
            }
            // grp-spec2
            {
                RouterSpec gs2 = gsit.next();
                Assert.assertEquals(version, gs2.getVersion());
                List<StreamRouterSpec> routeSpecs = gs2.getRouterSpecs();
                Assert.assertEquals(0, routeSpecs.size());
            }
        }
        // alert spec
        {
            Assert.assertEquals(2, status.getAlertSpecs().values().size());
            Iterator<AlertBoltSpec> asit = status.getAlertSpecs().values().iterator();
            // topo1
            {
                AlertBoltSpec alertSpec = asit.next();
                Assert.assertEquals(version, alertSpec.getVersion());
                String topo1 = alertSpec.getTopologyName();
                LOG.info("alert spec topology name {}", topo1);
                for (List<String> definitions : alertSpec.getBoltPolicyIdsMap().values()) {
                    Assert.assertEquals(1, definitions.size());
                    Assert.assertEquals(TEST_POLICY_1, definitions.get(0));
                }
            }
            // topo2
            {
                AlertBoltSpec alertSpec = asit.next();
                Assert.assertEquals(version, alertSpec.getVersion());
                String topo1 = alertSpec.getTopologyName();
                LOG.info("alert spec topology name {}", topo1);
                Assert.assertEquals(0, alertSpec.getBoltPolicyIdsMap().size());
            }
        }
    }

    private TopologyMgmtService createMgmtService() {
        TestTopologyMgmtService.BOLT_NUMBER = 5;
        TopologyMgmtService mgmtService = new TestTopologyMgmtService();
        return mgmtService;
    }

    private InMemScheduleConext createScheduleContext() {
        InMemScheduleConext context = new InMemScheduleConext();
        // topo
        Pair<Topology, TopologyUsage> pair1 = TestTopologyMgmtService.createEmptyTopology("topo1");
        Pair<Topology, TopologyUsage> pair2 = TestTopologyMgmtService.createEmptyTopology("topo2");
        context.addTopology(pair1.getLeft());
        context.addTopologyUsages(pair1.getRight());
        context.addTopology(pair2.getLeft());
        context.addTopologyUsages(pair2.getRight());

        // policy
        createSamplePolicy(context, TEST_POLICY_1, STREAM1);

        // data source
        Kafka2TupleMetadata ds = new Kafka2TupleMetadata();
        ds.setName(DS_NAME);
        ds.setTopic(TEST_TOPIC);
        ds.setCodec(new Tuple2StreamMetadata());
        context.addDataSource(ds);

        // schema
        {
            StreamDefinition schema = new StreamDefinition();
            {
                StreamColumn c = new StreamColumn();
                c.setName("col1");
                c.setType(Type.STRING);
                c.setDefaultValue("dflt");
                schema.getColumns().add(c);
            }
            {
                StreamColumn c = new StreamColumn();
                c.setName("col2");
                c.setType(Type.DOUBLE);
                c.setDefaultValue("0.0");
                schema.getColumns().add(c);
            }
            schema.setStreamId(STREAM1);
            schema.setValidate(false);
            schema.setDataSource(DS_NAME);
            context.addSchema(schema);
        }
        {
            StreamDefinition schema = new StreamDefinition();
            {
                StreamColumn c = new StreamColumn();
                c.setName("col1");
                c.setType(Type.STRING);
                c.setDefaultValue("dflt");
                schema.getColumns().add(c);
            }
            schema.setStreamId(STREAM2);
            schema.setValidate(false);
            schema.setDataSource(DS_NAME);
            context.addSchema(schema);
        }

        return context;
    }

    /**
     * Add policy after add policy
     */
    @Test
    public void test_schedule_add2() {
        IScheduleContext context = createScheduleContext();
        GreedyPolicyScheduler ps = new GreedyPolicyScheduler();
        TopologyMgmtService mgmtService = new TopologyMgmtService();
        ps.init(context, mgmtService);

        ScheduleOption option = new ScheduleOption();
        ps.schedule(option);
        ScheduleState status = ps.getState();
        context = ps.getContext(); // context updated!
        assertFirstPolicyScheduled(context, status);

        createSamplePolicy((InMemScheduleConext) context, TEST_POLICY_2, STREAM1);

        ps.init(context, mgmtService); // reinit
        ps.schedule(option);
        status = ps.getState();
        context = ps.getContext(); // context updated!
        // now assert two policy on the same queue
        assertSecondPolicyCreated(context, status);

        // add one policy on different stream of the same topic
        createSamplePolicy((InMemScheduleConext) context, TEST_POLICY_3, STREAM2);

        ps.init(context, mgmtService); // re-init
        ps.schedule(option);
        status = ps.getState();
        context = ps.getContext(); // context updated!
        assertThridPolicyScheduled(context, status);
    }

    private void assertThridPolicyScheduled(IScheduleContext context, ScheduleState status) {
        {
            // now assert two policy on the same queue
            Assert.assertEquals(2, status.getSpoutSpecs().values().size());
            Iterator<SpoutSpec> it = status.getSpoutSpecs().values().iterator();
            {
                // assert spout 1
                SpoutSpec ss = it.next();
                Assert.assertEquals(1, ss.getKafka2TupleMetadataMap().size());
                Assert.assertEquals(TEST_TOPIC, ss.getKafka2TupleMetadataMap().keySet().iterator().next());

                Assert.assertEquals(1, ss.getStreamRepartitionMetadataMap().size());
                List<StreamRepartitionMetadata> metas = ss.getStreamRepartitionMetadataMap().values().iterator().next();
                Assert.assertEquals(1, metas.size());

                StreamRepartitionMetadata streamMeta = metas.iterator().next();
                Assert.assertEquals(STREAM1, streamMeta.getStreamId());
                Assert.assertEquals(DS_NAME, streamMeta.getTopicName());
                Assert.assertEquals(1, streamMeta.groupingStrategies.size());

                StreamRepartitionStrategy gs = streamMeta.groupingStrategies.iterator().next();
                Assert.assertEquals(5, gs.numTotalParticipatingRouterBolts);
                Assert.assertEquals(5, gs.totalTargetBoltIds.size());
                Assert.assertEquals(0, gs.startSequence);

                PolicyAssignment pa1 = context.getPolicyAssignments().get(TEST_POLICY_1);
                PolicyAssignment pa2 = context.getPolicyAssignments().get(TEST_POLICY_2);
                Assert.assertNotNull(pa1);
                Assert.assertNotNull(pa2);
                Assert.assertEquals(pa1.getQueueId(), pa2.getQueueId());
            }
            {
                // assert spout 2
                SpoutSpec ss = it.next();
                Assert.assertEquals(1, ss.getKafka2TupleMetadataMap().size());

                Assert.assertEquals(TEST_TOPIC, ss.getKafka2TupleMetadataMap().keySet().iterator().next());

                Assert.assertEquals(1, ss.getStreamRepartitionMetadataMap().size());
                List<StreamRepartitionMetadata> metas = ss.getStreamRepartitionMetadataMap().values().iterator().next();
                Assert.assertEquals(1, metas.size());

                StreamRepartitionMetadata streamMeta = metas.iterator().next();
                Assert.assertEquals(STREAM2, streamMeta.getStreamId());
                Assert.assertEquals(DS_NAME, streamMeta.getTopicName());
                Assert.assertEquals(1, streamMeta.groupingStrategies.size());

                StreamRepartitionStrategy gs = streamMeta.groupingStrategies.iterator().next();
                Assert.assertEquals(5, gs.numTotalParticipatingRouterBolts);
                Assert.assertEquals(5, gs.totalTargetBoltIds.size());
                Assert.assertEquals(0, gs.startSequence);

                // assert policy assignment for the three policies
                PolicyAssignment pa1 = context.getPolicyAssignments().get(TEST_POLICY_1);
                PolicyAssignment pa2 = context.getPolicyAssignments().get(TEST_POLICY_2);
                PolicyAssignment pa3 = context.getPolicyAssignments().get(TEST_POLICY_3);
                Assert.assertNotNull(pa1);
                Assert.assertNotNull(pa2);
                Assert.assertNotNull(pa3);
                Assert.assertEquals(pa1.getQueueId(), pa2.getQueueId());
                Assert.assertNotEquals(pa1.getQueueId(), pa3.getQueueId());
                StreamWorkSlotQueue queue1 = getQueue(context, pa1.getQueueId()).getRight();
                StreamWorkSlotQueue queue3 = getQueue(context, pa3.getQueueId()).getRight();
                Assert.assertNotEquals(queue1.getWorkingSlots().get(0).getTopologyName(), queue3.getWorkingSlots().get(0).getTopologyName());
            }
        }
        // group spec
        {
            Iterator<RouterSpec> gsit = status.getGroupSpecs().values().iterator();
            Assert.assertEquals(2, status.getGroupSpecs().values().size());
            {
                // first topology's grp - spec
                gsit.next();
                // should be same with second policy scheduled, not assert here
            }
            {
                // second topology's grp - spec
                RouterSpec spec = gsit.next();
                Assert.assertEquals(1, spec.getRouterSpecs().size());
                StreamRouterSpec routeSpec = spec.getRouterSpecs().get(0);
                Assert.assertEquals(STREAM2, routeSpec.getStreamId());
                Assert.assertEquals(Arrays.asList("col1"), routeSpec.getPartition().getColumns());
            }
        }
        // alert spec
        {
            Assert.assertEquals(2, status.getAlertSpecs().values().size());
            Iterator<AlertBoltSpec> asit = status.getAlertSpecs().values().iterator();
            {
                // same to the two policy case, not assert here
                asit.next();
            }
            {
                // seconds topology's alert spec
                AlertBoltSpec as = asit.next();
                Assert.assertEquals(5, as.getBoltPolicyIdsMap().size());
                for (List<String> pdList : as.getBoltPolicyIdsMap().values()) {
                    Assert.assertEquals(1, pdList.size());
                    Assert.assertEquals(TEST_POLICY_3, pdList.get(0));
                }
            }
        }
    }

    private void assertSecondPolicyCreated(IScheduleContext context, ScheduleState status) {
        String version = status.getVersion();
        {
            // spout : assert two policy on the same topology (same worker
            // queue)
            Iterator<SpoutSpec> it = status.getSpoutSpecs().values().iterator();
            {
                // assert spout 1 has two policy
                SpoutSpec ss = it.next();
                Assert.assertEquals(1, ss.getKafka2TupleMetadataMap().size());
                Assert.assertEquals(TEST_TOPIC, ss.getKafka2TupleMetadataMap().keySet().iterator().next());

                Assert.assertEquals(1, ss.getStreamRepartitionMetadataMap().size());
                List<StreamRepartitionMetadata> metas = ss.getStreamRepartitionMetadataMap().values().iterator().next();
                Assert.assertEquals(1, metas.size());

                StreamRepartitionMetadata streamMeta = metas.iterator().next();
                Assert.assertEquals(STREAM1, streamMeta.getStreamId());
                Assert.assertEquals(DS_NAME, streamMeta.getTopicName());
                Assert.assertEquals(1, streamMeta.groupingStrategies.size());

                StreamRepartitionStrategy gs = streamMeta.groupingStrategies.iterator().next();
                Assert.assertEquals(5, gs.numTotalParticipatingRouterBolts);
                Assert.assertEquals(5, gs.totalTargetBoltIds.size());
                Assert.assertEquals(0, gs.startSequence);

                // assert two policy on the same queue
                PolicyAssignment pa1 = context.getPolicyAssignments().get(TEST_POLICY_1);
                PolicyAssignment pa2 = context.getPolicyAssignments().get(TEST_POLICY_2);
                Assert.assertNotNull(pa1);
                Assert.assertNotNull(pa2);
                Assert.assertEquals(pa1.getQueueId(), pa2.getQueueId());
                StreamWorkSlotQueue queue = getQueue(context, pa1.getQueueId()).getRight();
                Assert.assertNotNull(queue);
            }
            {
                // assert spout 2 is still empty
                SpoutSpec ss = it.next();
                Assert.assertEquals(0, ss.getKafka2TupleMetadataMap().size());
            }
        }

        // assert grp-by spec. This is nothing different compare to first policy
        {
            Iterator<RouterSpec> gsit = status.getGroupSpecs().values().iterator();
            {
                Assert.assertEquals(2, status.getGroupSpecs().values().size());
                Assert.assertTrue(gsit.hasNext());
                RouterSpec gspec = gsit.next();
                Assert.assertEquals(version, gspec.getVersion());
                String topo1 = gspec.getTopologyName();
                LOG.info("group spec topology name:", topo1);
                List<StreamRouterSpec> routeSpecs = gspec.getRouterSpecs();
                Assert.assertEquals(1, routeSpecs.size());
                for (StreamRouterSpec spec : routeSpecs) {
                    StreamPartition par = spec.getPartition();
                    Assert.assertEquals(STREAM1, par.getStreamId());
                    Assert.assertEquals(Arrays.asList("col1"), par.getColumns());
                    Assert.assertEquals(STREAM1, spec.getStreamId());

                    Assert.assertEquals(1, spec.getTargetQueue().size());
                    List<PolicyWorkerQueue> queues = spec.getTargetQueue();
                    Assert.assertEquals(1, queues.size());
                    Assert.assertEquals(5, queues.get(0).getWorkers().size());
                    for (WorkSlot slot : queues.get(0).getWorkers()) {
                        Assert.assertEquals(topo1, slot.getTopologyName());
                        LOG.info(slot.getBoltId());
                    }
                }
            }
            // grp-spec for second topology is still empty
            {
                RouterSpec gs2 = gsit.next();
                Assert.assertEquals(version, gs2.getVersion());
                List<StreamRouterSpec> routeSpecs = gs2.getRouterSpecs();
                Assert.assertEquals(0, routeSpecs.size());
            }
        }
        // alert spec
        {
            Assert.assertEquals(2, status.getAlertSpecs().values().size());
            Iterator<AlertBoltSpec> asit = status.getAlertSpecs().values().iterator();
            {
                AlertBoltSpec alertSpec = asit.next();
                Assert.assertEquals(version, alertSpec.getVersion());
                String topo1 = alertSpec.getTopologyName();
                LOG.info("alert spec topology name {}", topo1);
                for (List<String> definitions : alertSpec.getBoltPolicyIdsMap().values()) {
                    Assert.assertEquals(2, definitions.size());
//                    List<String> names = Arrays.asList(definitions.stream().map((t) -> t.getName()).toArray(String[]::new));
                    Assert.assertTrue(definitions.contains(TEST_POLICY_1));
                    Assert.assertTrue(definitions.contains(TEST_POLICY_2));
                }
            }
            // second spout
            {
                AlertBoltSpec spec = asit.next();
                Assert.assertEquals(0, spec.getBoltPolicyIdsMap().size());
            }
        }
    }

    public static Pair<MonitoredStream, StreamWorkSlotQueue> getQueue(IScheduleContext context, String queueId) {
        for (MonitoredStream ms : context.getMonitoredStreams().values()) {
            for (StreamWorkSlotQueue q : ms.getQueues()) {
                if (q.getQueueId().equals(queueId)) {
                    return Pair.of(ms, q);
                }
            }
        }
        return null;
    }

    @Test
    public void testGroupEquals() {
        StreamRepartitionStrategy gs1 = new StreamRepartitionStrategy();
        StreamPartition sp = new StreamPartition();
        sp.setColumns(Arrays.asList("col1"));
        sp.setSortSpec(new StreamSortSpec());
        sp.setStreamId("testStream");
        sp.setType(StreamPartition.Type.GROUPBY);
        gs1.partition = sp;

        StreamRepartitionStrategy gs2 = new StreamRepartitionStrategy();
        sp = new StreamPartition();
        sp.setColumns(Arrays.asList("col1"));
        sp.setSortSpec(new StreamSortSpec());
        sp.setStreamId("testStream");
        sp.setType(StreamPartition.Type.GROUPBY);
        gs2.partition = sp;

        Assert.assertTrue(gs1.equals(gs2));
        List<StreamRepartitionStrategy> list = new ArrayList<StreamRepartitionStrategy>();
        list.add(gs1);
        Assert.assertTrue(list.contains(gs2));
    }

    private void createSamplePolicy(InMemScheduleConext context, String policyName, String stream) {
        PolicyDefinition pd = new PolicyDefinition();
        pd.setParallelismHint(5);
        Definition def = new Definition();
        pd.setDefinition(def);
        pd.setName(policyName);
        pd.setInputStreams(Arrays.asList(stream));
        pd.setOutputStreams(Arrays.asList("outputStream2"));
        StreamPartition par = new StreamPartition();
        par.setColumns(Arrays.asList("col1"));
        par.setType(StreamPartition.Type.GLOBAL);
        par.setStreamId(stream);
        pd.setPartitionSpec(Arrays.asList(par));
        context.addPoilcy(pd);
    }

    /**
     * Add and remove
     */
    @Test
    public void test_schedule2_remove() {
        // TODO
    }

    @Test
    public void test_schedule_updateParitition() {
        // TODO
    }

    @Test
    public void test_schedule_nogroupby() {
        // TODO
    }

    @SuppressWarnings("unused")
    @Test
    public void test_schedule_multipleStream() throws Exception {
        IScheduleContext context = createScheduleContext();
        GreedyPolicyScheduler ps = new GreedyPolicyScheduler();
        TopologyMgmtService mgmtService = new TopologyMgmtService();

        createJoinPolicy((InMemScheduleConext) context, JOIN_POLICY_1, Arrays.asList(STREAM1, STREAM2));

        ps.init(context, mgmtService);
        ScheduleOption option = new ScheduleOption();
        ps.schedule(option);
        ScheduleState state = ps.getState();

        context = ps.getContext(); // context updated!
        // assert
        Assert.assertTrue(context.getPolicyAssignments().containsKey(JOIN_POLICY_1));
        Assert.assertTrue(context.getPolicyAssignments().containsKey(TEST_POLICY_1));
        PolicyAssignment pa1 = context.getPolicyAssignments().get(JOIN_POLICY_1);
        PolicyAssignment pa2 = context.getPolicyAssignments().get(TEST_POLICY_1);
        Assert.assertNotEquals(pa1.getQueueId(), pa2.getQueueId());

        StreamWorkSlotQueue joinPair = getQueue(context, pa1.getQueueId()).getRight();
        String joinTopo = joinPair.getWorkingSlots().get(0).topologyName;
        StreamWorkSlotQueue streamPair = getQueue(context, pa2.getQueueId()).getRight();
        String streamTopo = streamPair.getWorkingSlots().get(0).topologyName;
        Assert.assertNotEquals(joinTopo, streamTopo);

        // TODO more assert on state
        SpoutSpec joinSpout = state.getSpoutSpecs().get(joinTopo);
        RouterSpec groupSpec = state.getGroupSpecs().get(joinTopo);
        AlertBoltSpec alertSpec = state.getAlertSpecs().get(joinTopo);

        Assert.assertEquals(1, joinSpout.getStreamRepartitionMetadataMap().size());
        Assert.assertEquals(2, joinSpout.getStreamRepartitionMetadataMap().get(TEST_TOPIC).size());

        Assert.assertEquals(2, groupSpec.getRouterSpecs().size());

        LOG.info(new ObjectMapper().writeValueAsString(state));
    }

    private void createJoinPolicy(InMemScheduleConext context, String policyName, List<String> asList) {
        PolicyDefinition pd = new PolicyDefinition();
        pd.setParallelismHint(5);
        Definition def = new Definition();
        pd.setDefinition(def);
        pd.setName(policyName);
        pd.setInputStreams(asList);
        pd.setOutputStreams(Arrays.asList("outputStream2"));
        for (String streamId : pd.getInputStreams()) {
            StreamPartition par = new StreamPartition();
            par.setColumns(Arrays.asList("col1"));
            par.setType(StreamPartition.Type.GROUPBY);
            par.setStreamId(streamId);
            pd.addPartition(par);
        }
        context.addPoilcy(pd);
    }
}
