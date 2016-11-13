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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OverrideDeduplicatorSpecTest {

    @Test
    public void testOverrideDeduplicatorSpec() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kafka_broker", "localhost:9092");
        properties.put("topic", "TEST_TOPIC_NAME");
        OverrideDeduplicatorSpec overrideDeduplicatorSpec = new OverrideDeduplicatorSpec();
        overrideDeduplicatorSpec.setClassName("testClass");
        overrideDeduplicatorSpec.setProperties(properties);

        OverrideDeduplicatorSpec overrideDeduplicatorSpec1 = new OverrideDeduplicatorSpec();
        overrideDeduplicatorSpec1.setClassName("testClass");
        overrideDeduplicatorSpec1.setProperties(properties);

        Assert.assertFalse(overrideDeduplicatorSpec1 == overrideDeduplicatorSpec);
        Assert.assertTrue(overrideDeduplicatorSpec1.equals(overrideDeduplicatorSpec));
        Assert.assertTrue(overrideDeduplicatorSpec1.hashCode() == overrideDeduplicatorSpec.hashCode());

        overrideDeduplicatorSpec1.setClassName("testClass1");

        Assert.assertFalse(overrideDeduplicatorSpec1 == overrideDeduplicatorSpec);
        Assert.assertFalse(overrideDeduplicatorSpec1.equals(overrideDeduplicatorSpec));
        Assert.assertFalse(overrideDeduplicatorSpec1.hashCode() == overrideDeduplicatorSpec.hashCode());

        overrideDeduplicatorSpec1.setClassName("testClass");
        Map<String, String> properties1 = new HashMap<>();
        properties.put("kafka_broker", "localhost:9092");
        overrideDeduplicatorSpec1.setProperties(properties1);

        Assert.assertFalse(overrideDeduplicatorSpec1 == overrideDeduplicatorSpec);
        Assert.assertFalse(overrideDeduplicatorSpec1.equals(overrideDeduplicatorSpec));
        Assert.assertFalse(overrideDeduplicatorSpec1.hashCode() == overrideDeduplicatorSpec.hashCode());
    }

}
