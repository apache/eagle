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
package org.apache.eagle.alert.engine.absence;

import org.apache.eagle.alert.engine.evaluator.absence.AbsenceWindow;
import org.apache.eagle.alert.engine.evaluator.absence.AbsenceWindowProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Since 7/6/16.
 */
public class TestAbsenceWindowProcessor {
    @Test
    public void testDataMissing() {
        List<Object> expectedHosts = Arrays.asList("host1");
        AbsenceWindow window = new AbsenceWindow();
        window.startTime = 100L;
        window.endTime = 200L;
        AbsenceWindowProcessor processor = new AbsenceWindowProcessor(expectedHosts, window);
        processor.process(Arrays.asList("host2"), 90);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host3"), 101);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host3"), 138);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host2"), 189);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host2"), 201);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.ABSENT);
    }

    @Test(expected = IllegalStateException.class)
    public void testDataExists() {
        List<Object> expectedHosts = Arrays.asList("host1");
        AbsenceWindow window = new AbsenceWindow();
        window.startTime = 100L;
        window.endTime = 200L;
        AbsenceWindowProcessor processor = new AbsenceWindowProcessor(expectedHosts, window);
        processor.process(Arrays.asList("host2"), 90);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host3"), 101);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.NOT_SURE);
        processor.process(Arrays.asList("host1"), 138);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.OCCURRED);
        processor.process(Arrays.asList("host2"), 189);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.OCCURRED);
        processor.process(Arrays.asList("host2"), 201);
        Assert.assertEquals(processor.checkStatus(), AbsenceWindowProcessor.OccurStatus.OCCURRED);
        Assert.assertEquals(processor.checkExpired(), true);
        processor.process(Arrays.asList("host2"), 225);
    }
}