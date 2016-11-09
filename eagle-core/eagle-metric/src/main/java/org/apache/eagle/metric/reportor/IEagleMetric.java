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

import com.codahale.metrics.Gauge;

/**
 * It's just a workaround to extends Gauge instead of Metric interface
 * For MetricRegistry's notifyListenerOfRemovedMetric method will throw exception on unknown metric type.
 */

public interface IEagleMetric extends Gauge<Double> {

    void registerListener(EagleMetricListener listener);

    /**
     * return true if the metric need to be flushed, otherwise return false.
     *
     * @param value
     * @param userTimeClock
     * @return
     */
    boolean update(double value, long userTimeClock);
}
