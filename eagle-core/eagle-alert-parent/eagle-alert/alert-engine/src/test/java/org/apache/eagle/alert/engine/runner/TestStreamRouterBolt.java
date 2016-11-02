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
package org.apache.eagle.alert.engine.runner;

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
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.coordinator.impl.AbstractMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.apache.eagle.common.DateTimeUtil;

import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamRouterBolt {
    private final static Logger LOG = LoggerFactory.getLogger(TestStreamRouterBolt.class);

    /**
     * Mocked 5 Events
     * <p>
     * 1. Sent in random order:
     * "value1","value2","value3","value4","value5"
     * <p>
     * 2. Received correct time order and value5 is thrown because too late: "value2","value1","value3","value4"
     *
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testRouterWithSortAndRouteSpec() throws Exception {
        Config config = ConfigFactory.load();
        MockChangeService mockChangeService = new MockChangeService();
        StreamRouterBolt routerBolt = new StreamRouterBolt("routerBolt1", config, mockChangeService);

        final Map<String, List<PartitionedEvent>> streamCollected = new HashMap<>();
        final List<PartitionedEvent> orderCollected = new ArrayList<>();

        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            int count = 0;

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                PartitionedEvent event;
                try {
                    event = routerBolt.deserialize(tuple.get(0));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (count == 0) {
                    count++;
                }
                LOG.info(String.format("Collector received: [streamId=[%s], tuple=[%s] ", streamId, tuple));
                if (!streamCollected.containsKey(streamId)) {
                    streamCollected.put(streamId, new ArrayList<>());
                }
                streamCollected.get(streamId).add(event);
                orderCollected.add(event);
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

            @SuppressWarnings("unused")
            public void resetTimeout(Tuple input) {
            }

            @Override
            public void reportError(Throwable error) {
            }
        });

        Map stormConf = new HashMap<>();
        TopologyContext topologyContext = mock(TopologyContext.class);
        when(topologyContext.registerMetric(any(String.class), any(MultiCountMetric.class), any(int.class))).thenReturn(new MultiCountMetric());
        routerBolt.prepare(stormConf, topologyContext, collector);

        String streamId = "cpuUsageStream";
        // StreamPartition, groupby col1 for stream cpuUsageStream
        StreamPartition sp = new StreamPartition();
        sp.setStreamId(streamId);
        sp.setColumns(Collections.singletonList("col1"));
        sp.setType(StreamPartition.Type.GROUPBY);

        StreamSortSpec sortSpec = new StreamSortSpec();
//        sortSpec.setColumn("timestamp");
//        sortSpec.setOrder("asc");
        sortSpec.setWindowPeriod2(Period.minutes(1));
        sortSpec.setWindowMargin(1000);
        sp.setSortSpec(sortSpec);

        RouterSpec boltSpec = new RouterSpec();

        // set StreamRouterSpec to have 2 WorkSlot
        StreamRouterSpec routerSpec = new StreamRouterSpec();
        routerSpec.setPartition(sp);
        routerSpec.setStreamId(streamId);
        PolicyWorkerQueue queue = new PolicyWorkerQueue();
        queue.setPartition(sp);
        queue.setWorkers(Arrays.asList(new WorkSlot("testTopology", "alertBolt1"), new WorkSlot("testTopology", "alertBolt2")));
        routerSpec.setTargetQueue(Collections.singletonList(queue));
        boltSpec.addRouterSpec(routerSpec);
        boltSpec.setVersion("version1");

        // construct StreamDefinition
        StreamDefinition schema = new StreamDefinition();
        schema.setStreamId(streamId);
        StreamColumn column = new StreamColumn();
        column.setName("col1");
        column.setType(StreamColumn.Type.STRING);
        schema.setColumns(Collections.singletonList(column));
        Map<String, StreamDefinition> sds = new HashMap<>();
        sds.put(schema.getStreamId(), schema);

        routerBolt.declareOutputStreams(Arrays.asList("alertBolt1", "alertBolt2"));
        routerBolt.onStreamRouteBoltSpecChange(boltSpec, sds);
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        int taskId = 1;
        when(context.getComponentId(taskId)).thenReturn("comp1");
        when(context.getComponentOutputFields("comp1", "default")).thenReturn(new Fields("f0"));

        // =======================================
        // Mock 5 Events
        //
        // 1. Sent in random order:
        // "value1","value2","value3","value4","value5"
        //
        // 2. Received correct time order and value5 is thrown because too:
        // "value2","value1","value3","value4"
        // =======================================

        // construct event with "value1"
        StreamEvent event = new StreamEvent();
        event.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:01:30") * 1000);
        Object[] data = new Object[] {"value1"};
        event.setData(data);
        event.setStreamId(streamId);
        PartitionedEvent pEvent = new PartitionedEvent();
        pEvent.setEvent(event);
        pEvent.setPartition(sp);
        Tuple input = new TupleImpl(context, Collections.singletonList(pEvent), taskId, "default");
        routerBolt.execute(input);

        // construct another event with "value2"
        event = new StreamEvent();
        event.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:01:10") * 1000);
        data = new Object[] {"value2"};
        event.setData(data);
        event.setStreamId(streamId);
        pEvent = new PartitionedEvent();
        pEvent.setPartition(sp);
        pEvent.setEvent(event);
        input = new TupleImpl(context, Collections.singletonList(pEvent), taskId, "default");
        routerBolt.execute(input);

        // construct another event with "value3"
        event = new StreamEvent();
        event.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:01:40") * 1000);
        data = new Object[] {"value3"};
        event.setData(data);
        event.setStreamId(streamId);
        pEvent = new PartitionedEvent();
        pEvent.setPartition(sp);
        pEvent.setEvent(event);
        input = new TupleImpl(context, Collections.singletonList(pEvent), taskId, "default");
        routerBolt.execute(input);

        // construct another event with "value4"
        event = new StreamEvent();
        event.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:02:10") * 1000);
        data = new Object[] {"value4"};
        event.setData(data);
        event.setStreamId(streamId);
        pEvent = new PartitionedEvent();
        pEvent.setPartition(sp);
        pEvent.setEvent(event);
        input = new TupleImpl(context, Collections.singletonList(pEvent), taskId, "default");
        routerBolt.execute(input);

        // construct another event with "value5", which will be thrown because two late
        event = new StreamEvent();
        event.setTimestamp(DateTimeUtil.humanDateToSeconds("2016-01-01 00:00:10") * 1000);
        data = new Object[] {"value5"};
        event.setData(data);
        event.setStreamId(streamId);
        pEvent = new PartitionedEvent();
        pEvent.setPartition(sp);
        pEvent.setEvent(event);
        input = new TupleImpl(context, Collections.singletonList(pEvent), taskId, "default");
        routerBolt.execute(input);

        Assert.assertEquals("Should finally collect two streams", 2, streamCollected.size());
        Assert.assertTrue("Should collect stream stream_routerBolt_to_alertBolt1", streamCollected.keySet().contains(
            String.format(StreamIdConversion.generateStreamIdBetween(routerBolt.getBoltId(), "alertBolt1"))));
        Assert.assertTrue("Should collect stream stream_routerBolt_to_alertBolt2", streamCollected.keySet().contains(
            String.format(StreamIdConversion.generateStreamIdBetween(routerBolt.getBoltId(), "alertBolt2"))));

        Assert.assertEquals("Should finally collect 3 events", 3, orderCollected.size());
        Assert.assertArrayEquals("Should sort 3 events in ASC order", new String[] {"value2", "value1", "value3"}, orderCollected.stream().map((d) -> d.getData()[0]).toArray());

        // The first 3 events are ticked automatically by window

        routerBolt.cleanup();

        // Close will flush all events in memory, so will receive the last event which is still in memory as window is not expired according to clock
        // The 5th event will be thrown because too late and out of margin

        Assert.assertEquals("Should finally collect two streams", 2, streamCollected.size());
        Assert.assertEquals("Should finally collect 3 events", 4, orderCollected.size());
        Assert.assertArrayEquals("Should sort 4 events in ASC-ordered timestamp", new String[] {"value2", "value1", "value3", "value4"}, orderCollected.stream().map((d) -> d.getData()[0]).toArray());

    }

    @SuppressWarnings("serial")
    public static class MockChangeService extends AbstractMetadataChangeNotifyService {
        private final static Logger LOG = LoggerFactory.getLogger(MockChangeService.class);

        @Override
        public void close() throws IOException {
            LOG.info("Closing");
        }
    }
}
