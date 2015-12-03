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
package org.apache.eagle.metric.manager;

import org.apache.eagle.metric.Metric;
import org.apache.eagle.metric.report.MetricReport;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EagleMetricReportManager {

    private static EagleMetricReportManager manager = new EagleMetricReportManager();
    private Map<String, MetricReport> metricReportMap = new ConcurrentHashMap<>();

    private EagleMetricReportManager() {

    }

    public static EagleMetricReportManager getInstance () {
        return manager;
    }

    public boolean register(String name, MetricReport report) {
       if (metricReportMap.get(name) == null) {
           synchronized (metricReportMap) {
               if (metricReportMap.get(name) == null) {
                   metricReportMap.put(name, report);
                   return true;
               }
            }
        }
        return false;
    }

    public Map<String, MetricReport> getRegisteredReports() {
        return metricReportMap;
    }

    public void emit(List<Metric> list) {
        synchronized (this.metricReportMap) {
            for (MetricReport report : metricReportMap.values()) {
                report.emit(list);
            }
        }
    }
}
