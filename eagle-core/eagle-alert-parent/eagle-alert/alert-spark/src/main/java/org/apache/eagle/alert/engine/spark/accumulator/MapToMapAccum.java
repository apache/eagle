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

package org.apache.eagle.alert.engine.spark.accumulator;

import org.apache.spark.AccumulatorParam;

import java.util.HashMap;
import java.util.Map;

public class MapToMapAccum<T, K, V> implements AccumulatorParam<Map<T, Map<K, V>>> {
    @Override
    public Map<T, Map<K, V>> addAccumulator(Map<T, Map<K, V>> t1, Map<T, Map<K, V>> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<T, Map<K, V>> addInPlace(Map<T, Map<K, V>> r1, Map<T, Map<K, V>> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<T, Map<K, V>> zero(final Map<T, Map<K, V>> initialValue) {
        return new HashMap<>();
    }

    private Map<T, Map<K, V>> mergeMap(Map<T, Map<K, V>> map1, Map<T, Map<K, V>> map2) {
        Map<T, Map<K, V>> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> b));
        return result;
    }
}
