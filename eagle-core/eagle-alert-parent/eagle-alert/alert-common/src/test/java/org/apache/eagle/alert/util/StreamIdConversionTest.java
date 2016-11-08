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

package org.apache.eagle.alert.util;

import org.apache.eagle.alert.utils.StreamIdConversion;
import org.junit.Assert;
import org.junit.Test;

public class StreamIdConversionTest {
    @Test
    public void testGenerateStreamIdBetween() {
        String result = StreamIdConversion.generateStreamIdBetween("source1", "target1");
        Assert.assertEquals("stream_source1_to_target1", result);
        result = StreamIdConversion.generateStreamIdBetween("", "target1");
        Assert.assertEquals("stream__to_target1", result);
        result = StreamIdConversion.generateStreamIdBetween("source1", null);
        Assert.assertEquals("stream_source1_to_null", result);
    }

    @Test
    public void testGenerateStreamIdByPartition() {
        String result = StreamIdConversion.generateStreamIdByPartition(1);
        Assert.assertEquals("stream_1", result);
        result = StreamIdConversion.generateStreamIdByPartition(-1);
        Assert.assertEquals("stream_-1", result);
        result = StreamIdConversion.generateStreamIdByPartition(0);
        Assert.assertEquals("stream_0", result);

    }

}
