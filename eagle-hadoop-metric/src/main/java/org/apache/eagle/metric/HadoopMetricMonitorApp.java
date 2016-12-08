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
import org.apache.eagle.app.environment.builder.MetricDefinition;
import org.apache.eagle.app.environment.impl.StormEnvironment;

import java.util.Calendar;

public class HadoopMetricMonitorApp extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        return environment.newApp(config)
            .fromStream("HADOOP_JMX_METRIC_STREAM")
            .saveAsMetric(MetricDefinition
                .metricType("HADOOP_JMX_METRICS")
                .namedByField("metric")
                .description("Hadoop JMX Metrics for monitoring services like HDFS (Namenode/DataNode), YARN, etc.")
                .eventTimeByField("timestamp")
                .dimensionFields("host","component","site")
                .granularity(Calendar.MINUTE)
                .valueField("value"))
            .toTopology();
    }
}