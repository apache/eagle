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

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class StreamRepartitionStrategyTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStreamRepartitionStrategy() {
        thrown.expect(NullPointerException.class);
        StreamRepartitionStrategy streamRepartitionStrategy = new StreamRepartitionStrategy();
        streamRepartitionStrategy.hashCode();
    }

    @Test
    public void testStreamRepartitionStrategy1() {
        thrown.expect(NullPointerException.class);
        StreamRepartitionStrategy streamRepartitionStrategy = new StreamRepartitionStrategy();
        streamRepartitionStrategy.equals(streamRepartitionStrategy);
    }

    @Test
    public void testStreamRepartitionStrategy2() {

        StreamSortSpec streamSortSpec = new StreamSortSpec();
        streamSortSpec.setWindowPeriod("PT10S");
        StreamPartition streamPartition = new StreamPartition();
        List<String> columns = new ArrayList<>();
        columns.add("jobId");
        streamPartition.setColumns(columns);
        streamPartition.setSortSpec(streamSortSpec);
        streamPartition.setStreamId("test");
        streamPartition.setType(StreamPartition.Type.GROUPBY);


        StreamRepartitionStrategy streamRepartitionStrategy = new StreamRepartitionStrategy();
        Assert.assertEquals(null, streamRepartitionStrategy.getPartition());
        Assert.assertEquals(0, streamRepartitionStrategy.getNumTotalParticipatingRouterBolts());
        Assert.assertEquals(0, streamRepartitionStrategy.getStartSequence());
        streamRepartitionStrategy.setPartition(streamPartition);
        StreamRepartitionStrategy streamRepartitionStrategy1 = new StreamRepartitionStrategy();
        streamRepartitionStrategy1.setPartition(streamPartition);

        Assert.assertTrue(streamRepartitionStrategy.equals(streamRepartitionStrategy1));
        Assert.assertTrue(streamRepartitionStrategy.hashCode() == streamRepartitionStrategy1.hashCode());
    }
}
