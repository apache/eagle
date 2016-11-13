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

import com.codahale.metrics.Metric;
import org.apache.eagle.log.entity.GenericMetricEntity;

public class MetricEntityAdaptor {

    public static GenericMetricEntity convert(String name, Metric metric) {
        //TODO: add other type metric support
        EagleMetricKey metricName = MetricKeyCodeDecoder.decodeTSMetricKey(name);
        if (metric instanceof EagleCounterMetric) {
            EagleCounterMetric counter = (EagleCounterMetric) metric;
            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setPrefix(metricName.metricName);
            entity.setValue(new double[] {counter.getValue()});
            entity.setTags(metricName.tags);
            entity.setTimestamp(metricName.timestamp);
            return entity;
        } else if (metric instanceof EagleGaugeMetric) {
            EagleGaugeMetric gauge = (EagleGaugeMetric) metric;
            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setPrefix(metricName.metricName);
            entity.setValue(new double[] {gauge.getValue()});
            entity.setTags(metricName.tags);
            entity.setTimestamp(metricName.timestamp);
            return entity;
        }
        throw new RuntimeException("Not support this metric type for now!");
    }
}
