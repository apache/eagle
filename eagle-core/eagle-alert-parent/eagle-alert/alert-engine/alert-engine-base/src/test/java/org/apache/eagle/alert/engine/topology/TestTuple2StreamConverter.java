/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.eagle.alert.coordination.model.Tuple2StreamConverter;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.junit.Assert;
import org.junit.Test;

/**
 * Since 5/3/16.
 */
public class TestTuple2StreamConverter {
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void test(){
        Tuple2StreamMetadata metadata = new Tuple2StreamMetadata();
        Set activeStreamNames = new HashSet<>();
        activeStreamNames.add("defaultStringStream");
        metadata.setStreamNameSelectorCls("org.apache.eagle.alert.engine.scheme.PlainStringStreamNameSelector");
        metadata.setStreamNameSelectorProp(new Properties());
        metadata.getStreamNameSelectorProp().put("userProvidedStreamName", "defaultStringStream");
        metadata.setActiveStreamNames(activeStreamNames);
        metadata.setTimestampColumn("timestamp");
        Tuple2StreamConverter convert = new Tuple2StreamConverter(metadata);
        String topic = "testTopic";
        Map m = new HashMap<>();
        m.put("value", "IAmPlainString");
        long t = System.currentTimeMillis();
        m.put("timestamp", t);
        List<Object> ret = convert.convert(Arrays.asList(topic, m));
        Assert.assertEquals(topic, ret.get(0));
        Assert.assertEquals("defaultStringStream", ret.get(1));
        Assert.assertEquals(t, ret.get(2));
        Assert.assertEquals(m, ret.get(3));
    }
}
