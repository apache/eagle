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

package org.apache.eagle.alert.coordination.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamRouterSpecTest {
    @Test
    public void testStreamRouterSpec() {
        StreamRouterSpec streamRouterSpec = new StreamRouterSpec();
        Assert.assertEquals(null, streamRouterSpec.getPartition());
        Assert.assertEquals(null, streamRouterSpec.getStreamId());
        Assert.assertTrue(streamRouterSpec.getTargetQueue().isEmpty());

        List<WorkSlot> workers = new ArrayList<>();
        WorkSlot workSlot1 = new WorkSlot("setTopologyName1", "setBoltId1");
        WorkSlot workSlot2 = new WorkSlot("setTopologyName1", "setBoltId2");
        workers.add(workSlot1);
        workers.add(workSlot2);
        PolicyWorkerQueue policyWorkerQueue = new PolicyWorkerQueue(workers);
        streamRouterSpec.addQueue(policyWorkerQueue);
        streamRouterSpec.setStreamId("streamRouterSpec");

        Assert.assertEquals("streamRouterSpec", streamRouterSpec.getStreamId());
        Assert.assertEquals(1, streamRouterSpec.getTargetQueue().size());
        Assert.assertEquals(2, streamRouterSpec.getTargetQueue().get(0).getWorkers().size());

        StreamRouterSpec streamRouterSpec1 = new StreamRouterSpec();
        streamRouterSpec1.addQueue(policyWorkerQueue);
        streamRouterSpec1.setStreamId("streamRouterSpec1");

        Assert.assertFalse(streamRouterSpec.equals(streamRouterSpec1));

        streamRouterSpec1.setStreamId("streamRouterSpec");

        Assert.assertTrue(streamRouterSpec.equals(streamRouterSpec1));
        Assert.assertTrue(streamRouterSpec.hashCode() == streamRouterSpec1.hashCode());

    }
}
