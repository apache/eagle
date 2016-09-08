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
package org.apache.eagle.alert.coordinator.model;


/**
 * @since Mar 28, 2016
 */
public class GroupBoltUsage {

    private String boltId;
    private double load;

    public GroupBoltUsage(String boltId) {
        this.boltId = boltId;
    }

//    private final Set<String> streams = new HashSet<String>();
//    private final Map<String, StreamFilter> filters = new HashMap<String, StreamFilter>();

//    private final Map<String, List<StreamPartition>> groupByMeta;

    public double getLoad() {
        return load;
    }

    public void setLoad(double load) {
        this.load = load;
    }

//    public Set<String> getStreams() {
//        return streams;
//    }
//
//
//    public Map<String, StreamFilter> getFilters() {
//        return filters;
//    }

//    public Map<String, List<StreamPartition>> getGroupByMeta() {
//        return groupByMeta;
//    }

    public String getBoltId() {
        return boltId;
    }

    public void setBoltId(String boltId) {
        this.boltId = boltId;
    }

}
