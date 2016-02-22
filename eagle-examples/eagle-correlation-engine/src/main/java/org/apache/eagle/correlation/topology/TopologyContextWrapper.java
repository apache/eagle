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
package org.apache.eagle.correlation.topology;

import backtype.storm.metric.api.*;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;

import java.util.List;

/**
 * Created by yonzhang on 2/18/16.
 */
public class TopologyContextWrapper extends TopologyContext implements IMetricsContext {
    private TopologyContext delegate;
    private String topic;

    public TopologyContextWrapper(TopologyContext context, String topic) {
        super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        this.delegate = context;
        this.topic = topic;
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        return delegate.registerMetric(name + "_" + topic, metric, timeBucketSizeInSecs);
    }

    @Override
    public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
        return delegate.registerMetric(name, reducer, timeBucketSizeInSecs);
    }

    @Override
    public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
        return delegate.registerMetric(name, combiner, timeBucketSizeInSecs);
    }

    @Override
    public String getThisComponentId() {
        return delegate.getThisComponentId();
    }

    @Override
    public List<Integer> getComponentTasks(String componentId) {
        return delegate.getComponentTasks(componentId);
    }

    @Override
    public int getThisTaskId() {
        return delegate.getThisTaskId();
    }

    @Override
    public int getThisTaskIndex() {
        return delegate.getThisTaskIndex();
    }
}