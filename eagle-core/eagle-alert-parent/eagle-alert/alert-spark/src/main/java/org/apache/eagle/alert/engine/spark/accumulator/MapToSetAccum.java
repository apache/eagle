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

import java.util.*;


public class MapToSetAccum<T, V> implements AccumulatorParam<Map<T, Set<V>>> {

    @Override
    public Map<T, Set<V>> addAccumulator(Map<T, Set<V>> t1, Map<T, Set<V>> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<T, Set<V>> addInPlace(Map<T, Set<V>> r1, Map<T, Set<V>> r2) {
        return mergeMap(r1, r2);
    }

    @Override
    public Map<T, Set<V>> zero(Map<T, Set<V>> initialValue) {
        return new HashMap<>();
    }

    private Map<T, Set<V>> mergeMap(Map<T, Set<V>> map1, Map<T, Set<V>> map2) {
        Map<T, Set<V>> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> b));
        return result;
    }
}
