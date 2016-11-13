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

package org.apache.eagle.alert.engine.coordinator;

import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class StreamSortSpecTest {
    @Test
    public void testStreamSortSpec() {
        StreamSortSpec streamSortSpec = new StreamSortSpec();
        Assert.assertEquals(30000, streamSortSpec.getWindowMargin());
        Assert.assertEquals("", streamSortSpec.getWindowPeriod());
        Assert.assertEquals(0, streamSortSpec.getWindowPeriodMillis());
        Assert.assertEquals("StreamSortSpec[windowPeriod=,windowMargin=30000]", streamSortSpec.toString());
        streamSortSpec.setWindowPeriod("PT60S");
        Assert.assertEquals(60000, streamSortSpec.getWindowPeriodMillis());
        Assert.assertEquals("PT60S", streamSortSpec.getWindowPeriod());
        Assert.assertEquals("StreamSortSpec[windowPeriod=PT60S,windowMargin=30000]", streamSortSpec.toString());
        streamSortSpec.setWindowMargin(20);
        Assert.assertEquals(20, streamSortSpec.getWindowMargin());
        streamSortSpec.setWindowPeriodMillis(50000);
        Assert.assertEquals("StreamSortSpec[windowPeriod=PT50S,windowMargin=20]", streamSortSpec.toString());
        streamSortSpec.setWindowPeriod2(Period.minutes(10));
        Assert.assertEquals("StreamSortSpec[windowPeriod=PT10M,windowMargin=20]", streamSortSpec.toString());
        Assert.assertTrue(streamSortSpec.equals(new StreamSortSpec(streamSortSpec)));
        Assert.assertTrue(streamSortSpec.hashCode() == new StreamSortSpec(streamSortSpec).hashCode());
    }
}
