/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.router;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;

import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.impl.StormOutputCollector;
import org.apache.eagle.alert.engine.router.impl.StreamRouterBoltOutputCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.text.ParseException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestStreamRouterBoltOutputCollector {

    @Test
    public void testStreamRouterCollector() throws ParseException {
        String streamId = "HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX";
        StreamPartition partition = new StreamPartition();
        partition.setStreamId(streamId);
        partition.setType(StreamPartition.Type.GROUPBY);
        partition.setColumns(new ArrayList<String>() {{
            add("col1");
        }});

        // begin to create two router specs
        WorkSlot worker1 = new WorkSlot("ALERT_UNIT_TOPOLOGY_APP_SANDBOX", "alertBolt1");
        WorkSlot worker2 = new WorkSlot("ALERT_UNIT_TOPOLOGY_APP_SANDBOX", "alertBolt2");

        PolicyWorkerQueue queue1 = new PolicyWorkerQueue();
        queue1.setPartition(partition);
        queue1.setWorkers(new ArrayList<WorkSlot>() {
            {
                add(worker1);
            }
        });

        PolicyWorkerQueue queue2 = new PolicyWorkerQueue();
        queue2.setPartition(partition);
        queue2.setWorkers(new ArrayList<WorkSlot>() {
            {
                add(worker1);
                add(worker2);
            }
        });

        StreamRouterSpec spec1 = new StreamRouterSpec();
        spec1.setStreamId(streamId);
        spec1.setPartition(partition);

        spec1.setTargetQueue(new ArrayList<PolicyWorkerQueue>() {{
            add(queue1);
        }});

        StreamRouterSpec spec2 = new StreamRouterSpec();
        spec2.setStreamId(streamId);
        spec2.setPartition(partition);

        spec2.setTargetQueue(new ArrayList<PolicyWorkerQueue>() {{
            add(queue2);
        }});

        // the end of creating

        List<String> targetStreamIds = new ArrayList<>();
        IOutputCollector delegate = new IOutputCollector() {

            @Override
            public void reportError(Throwable error) {

            }

            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                targetStreamIds.add(streamId);
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

        };

        List<StreamRouterSpec> list1 = new ArrayList<>();
        list1.add(spec1);

        List<StreamRouterSpec> list2 = new ArrayList<>();
        list2.add(spec2);

        // construct StreamDefinition
        StreamDefinition schema = new StreamDefinition();
        schema.setStreamId(streamId);
        StreamColumn column = new StreamColumn();
        column.setName("col1");
        column.setType(StreamColumn.Type.STRING);
        schema.setColumns(Collections.singletonList(column));
        Map<String, StreamDefinition> sds = new HashMap<>();
        sds.put(schema.getStreamId(), schema);

        // create two events
        StreamEvent event1 = new StreamEvent();
        Object[] data = new Object[]{"value1"};
        event1.setData(data);
        event1.setStreamId(streamId);
        PartitionedEvent pEvent1 = new PartitionedEvent();
        pEvent1.setEvent(event1);
        pEvent1.setPartition(partition);

        StreamEvent event2 = new StreamEvent();
        Object[] data2 = new Object[]{"value3"};
        event2.setData(data2);
        event2.setStreamId(streamId);
        PartitionedEvent pEvent2 = new PartitionedEvent();
        pEvent2.setEvent(event2);
        pEvent2.setPartition(partition);

        TopologyContext context = Mockito.mock(TopologyContext.class);
        when(context.registerMetric(any(String.class), any(MultiCountMetric.class), any(int.class))).thenReturn(new MultiCountMetric());
        StreamContext streamContext = new StreamContextImpl(null, context.registerMetric("eagle.router", new MultiCountMetric(), 60), context);
        StreamRouterBoltOutputCollector collector = new StreamRouterBoltOutputCollector("test", new StormOutputCollector(new OutputCollector(delegate), null), null, streamContext);

        // add a StreamRouterSpec which has one worker
        collector.onStreamRouterSpecChange(list1, new ArrayList<>(), new ArrayList<>(), sds);
        collector.emit(pEvent1);
        Assert.assertTrue(targetStreamIds.size() == 1);

        // modify the StreamRouterSpec to contain two workers
        collector.onStreamRouterSpecChange(new ArrayList<>(), new ArrayList<>(), list2, sds);
        collector.emit(pEvent2);
        Assert.assertTrue(targetStreamIds.size() == 2);
    }
}
