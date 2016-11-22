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

package org.apache.eagle.jpm.util.jobcounter;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JobCountersSerDeserTest {
    @Test
    public void testSerializeAndDeserialize() {
        JobCountersSerDeser jobCountersSerDeser = new JobCountersSerDeser();
        JobCounters jobCounters = new JobCounters();
        Map<String, Map<String, Long>> counters = new HashMap<>();
        Map<String, Long> taskCounters = new HashMap<>();
        taskCounters.put(JobCounters.CounterName.CPU_MILLISECONDS.getName(), 18l);
        taskCounters.put(JobCounters.CounterName.GC_MILLISECONDS.getName(), 17l);
        taskCounters.put(JobCounters.CounterName.SPLIT_RAW_BYTES.getName(), 1l);
        counters.put(JobCounters.GroupName.MapReduceTaskCounter.getName(), taskCounters);
        jobCounters.setCounters(counters);

        JobCounters jobCounters1 = new JobCounters();
        jobCounters1.setCounters(jobCountersSerDeser.deserialize(jobCountersSerDeser.serialize(jobCounters)).getCounters());

        Assert.assertEquals(18l,jobCounters1.getCounterValue(JobCounters.CounterName.CPU_MILLISECONDS).longValue());
        Assert.assertEquals(17l,jobCounters1.getCounterValue(JobCounters.CounterName.GC_MILLISECONDS).longValue());
        Assert.assertEquals(1l,jobCounters1.getCounterValue(JobCounters.CounterName.SPLIT_RAW_BYTES).longValue());
    }
}
