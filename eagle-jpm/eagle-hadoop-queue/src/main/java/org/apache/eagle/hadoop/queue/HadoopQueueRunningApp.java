/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.hadoop.queue;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.messaging.StormStreamSink;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataSource;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueMetricPersistBolt;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueRunningSpout;

import java.util.HashMap;
import java.util.Map;

public class HadoopQueueRunningApp extends StormApplication {
    public StormTopology execute(Config config, StormEnvironment environment) {
        HadoopQueueRunningAppConfig appConfig = new HadoopQueueRunningAppConfig(config);

        String spoutName = "runningQueueSpout";
        String persistBoltName = "persistBolt";

        IRichSpout spout = new HadoopQueueRunningSpout(appConfig);

        //String acceptedAppStreamId = persistBoltName + "-to-" + DataSource.RUNNING_APPS.toString();
        //String schedulerStreamId = persistBoltName + "-to-" + DataSource.SCHEDULER.toString();
        //streamMaps.put(DataSource.RUNNING_APPS, acceptedAppStreamId);
        //streamMaps.put(DataSource.SCHEDULER, schedulerStreamId);

        int numOfPersistTasks = appConfig.topology.numPersistTasks;
        int numOfSinkTasks = appConfig.topology.numSinkTasks;
        int numOfSpoutTasks = 1;

        HadoopQueueMetricPersistBolt bolt = new HadoopQueueMetricPersistBolt(appConfig);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(spoutName, spout, numOfSpoutTasks).setNumTasks(numOfSpoutTasks);
        builder.setBolt(persistBoltName, bolt, numOfPersistTasks).setNumTasks(numOfPersistTasks).shuffleGrouping(spoutName);

        StormStreamSink queueSinkBolt = environment.getStreamSink("HADOOP_QUEUE_STREAM", config);
        builder.setBolt("queueKafkaSink", queueSinkBolt, numOfSinkTasks)
                .setNumTasks(numOfSinkTasks).shuffleGrouping(persistBoltName);

        //StormStreamSink appSinkBolt = environment.getStreamSink("ACCEPTED_APP_STREAM", config);
        //builder.setBolt("appKafkaSink", appSinkBolt, numOfSinkTasks)
        //        .setNumTasks(numOfSinkTasks).shuffleGrouping(persistBoltName, acceptedAppStreamId);

        return builder.createTopology();
    }
}
