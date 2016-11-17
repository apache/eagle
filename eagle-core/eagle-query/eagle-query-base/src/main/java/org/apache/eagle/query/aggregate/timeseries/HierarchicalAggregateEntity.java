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
package org.apache.eagle.query.aggregate.timeseries;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.*;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class HierarchicalAggregateEntity {
    private String key;
    private List<GroupbyBucket.Function> tmpValues = new ArrayList<GroupbyBucket.Function>();
    private List<Double> values = new ArrayList<Double>();
    private SortedMap<String, HierarchicalAggregateEntity> children = new TreeMap<String, HierarchicalAggregateEntity>();
    private SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> sortedList = null;

    public SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> getSortedList() {
        return sortedList;
    }

    public void setSortedList(
        SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> sortedList) {
        this.sortedList = sortedList;
    }

    public List<GroupbyBucket.Function> getTmpValues() {
        return tmpValues;
    }

    public void setTmpValues(List<GroupbyBucket.Function> tmpValues) {
        this.tmpValues = tmpValues;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Double> getValues() {
        return values;
    }

    public void setValues(List<Double> values) {
        this.values = values;
    }

    public SortedMap<String, HierarchicalAggregateEntity> getChildren() {
        return children;
    }

    public void setChildren(SortedMap<String, HierarchicalAggregateEntity> children) {
        this.children = children;
    }
}
