/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.metric.reportor;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class EagleMetric implements IEagleMetric {

    public long latestUserTimeClock;
    public AtomicDouble value;
    public String name;
    public long granularity;
    public List<EagleMetricListener> metricListeners = new ArrayList<>();
    private static final Logger LOG = LoggerFactory.getLogger(EagleCounterMetric.class);

    public EagleMetric(EagleMetric metric) {
        this.latestUserTimeClock = metric.latestUserTimeClock;
        this.name = metric.name;
        this.value = new AtomicDouble(metric.value.doubleValue());
        this.granularity = metric.granularity;
    }

    public EagleMetric(long latestUserTimeClock, String name, double value, long granularity) {
        this.latestUserTimeClock = latestUserTimeClock;
        this.name = name;
        this.value = new AtomicDouble(value);
        this.granularity = granularity;
    }

    public EagleMetric(long latestUserTimeClock, String metricName, double value) {
        this(latestUserTimeClock, metricName, value, 5 * DateUtils.MILLIS_PER_MINUTE);
    }

    public void registerListener(EagleMetricListener listener) {
        metricListeners.add(listener);
    }

    public Double getValue() {
        return value.doubleValue();
    }
}
