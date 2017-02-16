/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.aggregation;

import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.jpm.aggregation.mr.MRMetricsAggregateContainer;
import org.apache.eagle.jpm.aggregation.storm.AggregationBolt;
import org.apache.eagle.jpm.aggregation.storm.AggregationSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;

import java.util.*;

public class AggregationApplication extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        List<String> metricNames = new ArrayList<>();
        String[] metricNamesArr = config.getString("aggregate.counters.metrics").split(",");
        for (int i = 0; i < metricNamesArr.length; i++) {
            metricNames.add(metricNamesArr[i].trim().toLowerCase());
        }
        List<String> groupByColumns = new ArrayList<>();

        String[] groupByColumnsArr = config.getString("aggregate.counters.groupBys").split(";");
        for (int i = 0; i < groupByColumnsArr.length; i++) {
            groupByColumns.add(groupByColumnsArr[i].trim());
        }

        Map<String, List<List<String>>> metrics = new HashMap<>();
        for (String metric : metricNames) {
            metrics.put(metric, new ArrayList<>());
            for (String cols : groupByColumns) {
                metrics.get(metric).add(Arrays.asList(cols.replaceAll(" ", "").split(",")));
            }
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutName = "mrHistoryAggregationSpout";
        String boltName = "mrHistoryAggregationBolt";
        AggregationConfig aggregationConfig = AggregationConfig.newInstance(config);
        int tasks = aggregationConfig.getConfig().getInt("stormConfig." + spoutName + "Tasks");
        topologyBuilder.setSpout(
            spoutName,
            new AggregationSpout(aggregationConfig, new MRMetricsAggregateContainer(metrics, aggregationConfig)),
            tasks
        ).setNumTasks(tasks);

        tasks = aggregationConfig.getConfig().getInt("stormConfig." + boltName + "Tasks");
        topologyBuilder.setBolt(boltName,
            new AggregationBolt(aggregationConfig.getStormConfig(), new MRMetricsAggregateContainer(metrics, aggregationConfig)),
            tasks).setNumTasks(tasks).shuffleGrouping(spoutName);

        return topologyBuilder.createTopology();
    }
}
