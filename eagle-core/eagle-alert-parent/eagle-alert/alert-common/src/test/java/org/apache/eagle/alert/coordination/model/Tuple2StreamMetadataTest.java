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

import java.util.*;

public class Tuple2StreamMetadataTest {
    @Test
    public void testTuple2StreamMetadata() {
        Tuple2StreamMetadata metadata = new Tuple2StreamMetadata();
        Set activeStreamNames = new HashSet<>();
        activeStreamNames.add("defaultStringStream");
        metadata.setStreamNameSelectorCls("org.apache.eagle.alert.engine.scheme.PlainStringStreamNameSelector");
        metadata.setStreamNameSelectorProp(new Properties());
        metadata.getStreamNameSelectorProp().put("userProvidedStreamName", "defaultStringStream");
        metadata.setActiveStreamNames(activeStreamNames);
        metadata.setTimestampColumn("timestamp");

        Tuple2StreamMetadata metadata1 = new Tuple2StreamMetadata();
        Set activeStreamNames1 = new HashSet<>();
        activeStreamNames1.add("defaultStringStream");
        metadata1.setStreamNameSelectorCls("org.apache.eagle.alert.engine.scheme.PlainStringStreamNameSelector");
        metadata1.setStreamNameSelectorProp(new Properties());
        metadata1.getStreamNameSelectorProp().put("userProvidedStreamName", "defaultStringStream");
        metadata1.setActiveStreamNames(activeStreamNames1);
        metadata1.setTimestampColumn("timestamp");

        Assert.assertFalse(metadata == metadata1);
        Assert.assertFalse(metadata.equals(metadata1));
        Assert.assertFalse(metadata.hashCode() == metadata1.hashCode());
    }
}
