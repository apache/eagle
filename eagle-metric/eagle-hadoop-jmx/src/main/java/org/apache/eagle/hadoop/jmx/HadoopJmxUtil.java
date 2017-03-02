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

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.METRIC_LIVE_RATIO_NAME_FORMAT;
import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.ROLE_TAG;
import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.SITE_TAG;

public class HadoopJmxUtil {

    public static GenericMetricEntity generateMetric(String role, double value, String site, long timestamp) {
        Map<String, String> tags = new HashMap<>();
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, role);
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


    public static class JmxMetricBuilder {
        private long timestamp;
        private String metric;
        private double value;
        private String host;
        private String component;
        private String site;

        public JmxMetricBuilder setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public JmxMetricBuilder setMetric(String metric) {
            this.metric = metric;
            return this;
        }

        public JmxMetricBuilder setValue(double value) {
            this.value = value;
            return this;
        }

        public JmxMetricBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public JmxMetricBuilder setComponent(String component) {
            this.component = component;
            return this;
        }

        public JmxMetricBuilder setSite(String site) {
            this.site = site;
            return this;
        }

        public Map<String, Object> build() {
            Map<String, Object> map = new HashMap<>();
            map.put("timestamp", timestamp);
            map.put("metric", metric);
            map.put("value", value);
            map.put("host", host);
            map.put("component", component);
            map.put("site", site);
            return map;
        }
    }
}
