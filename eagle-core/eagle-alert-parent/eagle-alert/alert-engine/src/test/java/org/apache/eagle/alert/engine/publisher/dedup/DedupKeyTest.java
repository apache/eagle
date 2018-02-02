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

package org.apache.eagle.alert.engine.publisher.dedup;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DedupKeyTest {

    @Test
    public void test() {
        Map<DedupKey, Integer> testMap = new HashMap<>();
        DedupKey key1 = new DedupKey("policy1", "stream1");
        update(testMap, key1);
        update(testMap, key1);

        DedupKey key2 = new DedupKey("policy2", "stream2");
        update(testMap, key2);

        Assert.assertTrue(testMap.get(key1) == 1);
        Assert.assertTrue(testMap.get(key2) == 0);

        DedupKey key3 = new DedupKey("policy1", "stream1");
        update(testMap, key3);

        Assert.assertTrue(testMap.get(key3) == 2);
    }

    private void update(Map<DedupKey, Integer> map, DedupKey key) {
        if (map.containsKey(key)) {
            map.put(key, map.get(key) + 1);
        } else {
            map.put(key, 0);
        }
    }
}


