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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class EagleCounterMetric extends EagleMetric {

    private static final Logger LOG = LoggerFactory.getLogger(EagleCounterMetric.class);

    public EagleCounterMetric(long latestUserTimeClock, String name, double value, long granularity) {
        super(latestUserTimeClock, name, value, granularity);
    }

    public EagleCounterMetric(EagleCounterMetric metric) {
        super(metric);
    }

    public long trim(long latestUserTimeClock) {
        return latestUserTimeClock / granularity * granularity;
    }

    public void flush(long latestUserTimeClock) {
        for (EagleMetricListener listener : metricListeners) {
            EagleCounterMetric newEagleMetric = new EagleCounterMetric(this);
            newEagleMetric.name = MetricKeyCodeDecoder.addTimestampToMetricKey(trim(latestUserTimeClock), newEagleMetric.name);
            listener.onMetricFlushed(Collections.singletonList(newEagleMetric));
        }
    }

    public boolean checkIfNeedFlush(long currentUserTimeClock) {
        return currentUserTimeClock - latestUserTimeClock > granularity;
    }

    public boolean update(double d, long currentUserTimeClock) {
        Boolean readyToFlushed = checkIfNeedFlush(currentUserTimeClock);
        if (!readyToFlushed) {
            if (currentUserTimeClock < latestUserTimeClock) {
                LOG.warn("Something must be wrong, event should come in order of userTimeClock");
            }
            value.addAndGet(d);
        } else {
            flush(latestUserTimeClock);
            value.getAndSet(1);
            latestUserTimeClock = currentUserTimeClock;
        }
        return readyToFlushed;
    }
}
