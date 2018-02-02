/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.alert.engine.coordinator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class AlertDeduplicationTest {

    @Test
    public void testEqual() {
        AlertDeduplication deduplication1 = new AlertDeduplication();
        deduplication1.setDedupIntervalMin("1");
        deduplication1.setOutputStreamId("stream");

        AlertDeduplication deduplication2 = new AlertDeduplication();
        deduplication2.setDedupIntervalMin("1");
        deduplication2.setOutputStreamId("stream");
        deduplication2.setDedupFields(new ArrayList<>());

        Assert.assertFalse(deduplication1.equals(deduplication2));

        AlertDeduplication deduplication3 = new AlertDeduplication();
        deduplication3.setDedupFields(new ArrayList<>());
        deduplication3.setOutputStreamId("stream");
        deduplication3.setDedupIntervalMin("1");

        Assert.assertTrue(deduplication3.equals(deduplication2));
    }
}
