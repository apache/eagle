/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metric;

import backtype.storm.generated.StormTopology;
import com.typesafe.config.Config;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.builder.MetricDescriptor;
import org.apache.eagle.app.environment.builder.MetricDescriptor.MetricGroupSelector;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.utils.AppConfigUtils;

import java.util.Calendar;

public class HadoopMetricMonitorApp extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        return environment.newApp(config)
                .fromStream("HADOOP_JMX_METRIC_STREAM")
                .saveAsMetric(
                        MetricDescriptor.metricGroupAs((MetricGroupSelector) event -> {
                            if (event.containsKey("component")) {
                                return String.format("hadoop.%s", ((String) event.get("component")).toLowerCase());
                            } else {
                                return "hadoop.metrics";
                            }
                        })
                        .siteAs(AppConfigUtils.getSiteId(config))
                        .namedByField("metric")
                        .eventTimeByField("timestamp")
                        .dimensionFields("host", "component", "site")
                        .granularity(Calendar.MINUTE)
                        .valueField("value"))
                .fromStream("SYSTEM_METRIC_STREAM")
                .saveAsMetric(MetricDescriptor.metricGroupByField("group")
                        .siteAs(AppConfigUtils.getSiteId(config))
                        .namedByField("metric")
                        .eventTimeByField("timestamp")
                        .dimensionFields("host", "group", "site", "device")
                        .granularity(Calendar.MINUTE)
                        .valueField("value")
                )
                .toTopology();
    }
}