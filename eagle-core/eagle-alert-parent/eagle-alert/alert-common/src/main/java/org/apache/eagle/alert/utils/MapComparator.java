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

package org.apache.eagle.alert.utils;

import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public class MapComparator<K, V> {
    private Map<K, V> map1;
    private Map<K, V> map2;
    private List<V> added = new ArrayList<>();
    private List<V> removed = new ArrayList<>();
    private List<V> modified = new ArrayList<>();
    private List<K> addedKeys = new ArrayList<>();
    private List<K> removedKeys = new ArrayList<>();
    private List<K> modifiedKeys = new ArrayList<>();

    public MapComparator(Map<K, V> map1, Map<K, V> map2) {
        this.map1 = map1;
        this.map2 = map2;
    }

    @SuppressWarnings("unchecked")
    public void compare() {
        Set<K> keys1 = map1.keySet();
        Set<K> keys2 = map2.keySet();
        addedKeys = new ArrayList<>(CollectionUtils.subtract(keys1, keys2));
        removedKeys = new ArrayList<>(CollectionUtils.subtract(keys2, keys1));
        modifiedKeys = new ArrayList<>(CollectionUtils.intersection(keys1, keys2));

        addedKeys.forEach(k -> added.add(map1.get(k)));
        removedKeys.forEach(k -> removed.add(map2.get(k)));
        modifiedKeys.forEach(k -> {
            if (!map1.get(k).equals(map2.get(k))) {
                modified.add(map1.get(k));
            }
        });
    }

    public List<V> getAdded() {
        return added;
    }

    public List<V> getRemoved() {
        return removed;
    }

    public List<V> getModified() {
        return modified;
    }

    public List<K> getAddedKeys() {
        return addedKeys;
    }

    public List<K> getRemovedKeys() {
        return removedKeys;
    }

    public List<K> getModifiedKeys() {
        return modifiedKeys;
    }
}
