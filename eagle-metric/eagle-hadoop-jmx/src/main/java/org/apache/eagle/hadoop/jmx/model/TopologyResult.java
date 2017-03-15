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

package org.apache.eagle.hadoop.jmx.model;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;

import java.util.ArrayList;
import java.util.List;

public class TopologyResult {
    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private List<GenericMetricEntity> metrics = new ArrayList<>();

    public List<TaggedLogAPIEntity> getEntities() {
        return entities;
    }

    public void setEntities(List<TaggedLogAPIEntity> entities) {
        this.entities = entities;
    }

    public List<GenericMetricEntity> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<GenericMetricEntity> metrics) {
        this.metrics = metrics;
    }

}
