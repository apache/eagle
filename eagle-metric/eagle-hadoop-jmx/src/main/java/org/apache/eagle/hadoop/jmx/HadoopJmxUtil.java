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

package org.apache.eagle.hadoop.jmx;

import org.apache.eagle.log.entity.GenericMetricEntity;

import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.*;

public class HadoopJmxUtil {

    public static GenericMetricEntity generateMetric(String role, double value, String site, long timestamp) {
        Map<String, String> tags = new HashMap<>();
        tags.put(SITE_TAG, site);
        tags.put(COMPONENT_TAG, role);
        String metricName = String.format(METRIC_LIVE_RATIO_NAME_FORMAT, role);
        return buildGenericMetric(timestamp, metricName, value, tags);
    }

    private static GenericMetricEntity buildGenericMetric(Long timestamp,
                                                          String metricName,
                                                          double value,
                                                          Map<String, String> tags) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        metricEntity.setTimestamp(timestamp);
        metricEntity.setTags(tags);
        metricEntity.setPrefix(metricName);
        metricEntity.setValue(new double[] {value});
        return metricEntity;
    }

}
