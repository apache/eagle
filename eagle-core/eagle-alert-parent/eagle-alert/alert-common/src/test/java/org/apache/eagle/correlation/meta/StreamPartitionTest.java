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
package org.apache.eagle.correlation.meta;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class StreamPartitionTest {
    @Test
    public void testStreamPartitionEqual(){
        StreamPartition partition1 = new StreamPartition();
        partition1.setStreamId("unittest");
        partition1.setColumns(Arrays.asList("col1","col2"));
        StreamPartition partition2 = new StreamPartition();
        partition2.setStreamId("unittest");
        partition2.setColumns(Arrays.asList("col1","col2"));
        Assert.assertTrue(partition1.equals(partition2));
    }
}
