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
import org.apache.eagle.app.environment.builder.CounterToRateFunction;
import org.apache.eagle.app.environment.builder.MetricDefinition;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.utils.ClockWithOffset;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class HadoopMetricMonitorApp extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {

        MetricDefinition metricDefinition = MetricDefinition
                .metricType("HADOOP_JMX_METRICS")
                .namedByField("metric")
                .eventTimeByField("timestamp")
                .dimensionFields("host", "component", "site")
                .granularity(Calendar.MINUTE)
                .valueField("value");

        return environment.newApp(config)
                .fromStream("HADOOP_JMX_METRIC_STREAM")
                .transformBy(new CounterToRateFunction(metricDefinition, 3, TimeUnit.MINUTES, ClockWithOffset.INSTANCE))
                .saveAsMetric(metricDefinition)
                .toTopology();
    }
}