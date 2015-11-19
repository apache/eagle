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
package org.apache.eagle.metric;

import java.util.HashMap;
import java.util.Map;

import com.google.common.util.concurrent.AtomicDouble;

/**
 */
public class CountingMetric extends Metric{

    public CountingMetric(long timestamp, Map<String, String> dimensions, String metricName, AtomicDouble value) {
    	super(timestamp, dimensions, metricName, value);
    }
  
    public CountingMetric(long timestamp, Map<String, String> dimensions, String metricName) {
	   this(timestamp, dimensions, metricName, new AtomicDouble(0.0));
    }

    public CountingMetric(CountingMetric metric) {
        this(metric.timestamp, new HashMap<String, String>(metric.dimensions), metric.metricName, metric.value);
    }

    @Override
    public double update(double delta) {
    	return value.addAndGet(delta);
    }
}
