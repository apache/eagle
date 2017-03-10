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
package org.apache.eagle.alert.engine.evaluator.impl;

import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.StreamCounter;
import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.router.StreamOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

public class AlertBoltOutputCollectorWrapperTest {

    private AlertBoltOutputCollectorWrapper alertBoltOutputCollectorWrapper;

    // mock objects
    private StreamOutputCollector outputCollector;
    private Object outputLock;
    private StreamContext streamContext;
    private StreamCounter streamCounter;

    private Set<PublishPartition> publishPartitions = new HashSet<>();

    private static final String samplePublishId = "samplePublishId";
    private static final String samplePublishId2 = "samplePublishId2";
    private static final String samplePolicyId = "samplePolicyId";
    private static final String sampleStreamId = "sampleStreamId";
    private static final String sampleStreamId2 = "sampleStreamId2";

    @Before
    public void setUp() throws Exception {
        outputCollector = mock(StreamOutputCollector.class);
        outputLock = mock(Object.class);
        streamContext = mock(StreamContext.class);
        streamCounter = mock(StreamCounter.class);
        alertBoltOutputCollectorWrapper = new AlertBoltOutputCollectorWrapper(outputCollector, outputLock, streamContext);
    }

    @Before
    public void tearDown() throws Exception {
        alertBoltOutputCollectorWrapper.onAlertBoltSpecChange(new HashSet<>(), publishPartitions, new HashSet<>());
        publishPartitions.clear();
    }

    @Test
    public void testNormal() throws Exception {
        doReturn(streamCounter).when(streamContext).counter();

        publishPartitions.add(createPublishPartition(samplePublishId, samplePolicyId, sampleStreamId));
        publishPartitions.add(createPublishPartition(samplePublishId2, samplePolicyId, sampleStreamId2));
        alertBoltOutputCollectorWrapper.onAlertBoltSpecChange(publishPartitions, new HashSet<>(), new HashSet<>());

        AlertStreamEvent event = new AlertStreamEvent();
        event.setPolicyId(samplePolicyId);
        StreamDefinition sd = new StreamDefinition();
        sd.setStreamId(sampleStreamId);
        sd.setColumns(new ArrayList<>());
        event.setSchema(sd);

        alertBoltOutputCollectorWrapper.emit(event);

        verify(streamCounter, times(1)).incr(anyString());
        verify(outputCollector, times(1)).emit(anyObject());
    }

    @Test
    public void testExceptional() throws Exception {
        doReturn(streamCounter).when(streamContext).counter();

        publishPartitions.add(createPublishPartition(samplePublishId, samplePolicyId, sampleStreamId));
        publishPartitions.add(createPublishPartition(samplePublishId, samplePolicyId, sampleStreamId));
        alertBoltOutputCollectorWrapper.onAlertBoltSpecChange(publishPartitions, new HashSet<>(), new HashSet<>());

        AlertStreamEvent event = new AlertStreamEvent();
        event.setPolicyId(samplePolicyId);
        StreamDefinition sd = new StreamDefinition();
        sd.setStreamId(sampleStreamId);
        sd.setColumns(new ArrayList<>());
        event.setSchema(sd);

        alertBoltOutputCollectorWrapper.emit(event);

        verify(streamCounter, times(1)).incr(anyString());
        verify(outputCollector, times(1)).emit(anyObject());
    }

    private PublishPartition createPublishPartition(String publishId, String policyId, String streamId) {
        PublishPartition publishPartition = new PublishPartition();
        publishPartition.setPolicyId(policyId);
        publishPartition.setStreamId(streamId);
        publishPartition.setPublishId(publishId);
        publishPartition.setColumns(new HashSet<>());
        return publishPartition;
    }

}
