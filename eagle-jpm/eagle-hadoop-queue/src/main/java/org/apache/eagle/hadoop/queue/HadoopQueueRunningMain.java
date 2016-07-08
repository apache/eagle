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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.hadoop.queue;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueMetricPersistBolt;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueRunningSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopQueueRunningMain {

    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String TOTAL_WORKER_NUM = "topology.numOfTotalWorkers";
    public final static String TOPOLOGY_NAME = "topology.name";
    public final static String LOCAL_MODE = "topology.localMode";

    public static void main(String [] args) {
        //System.setProperty("config.resource", "/application.conf");
        Config config = ConfigFactory.load();

        IRichSpout spout = new HadoopQueueRunningSpout(config);
        HadoopQueueMetricPersistBolt bolt = new HadoopQueueMetricPersistBolt(config);
        TopologyBuilder builder = new TopologyBuilder();

        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfTotalWorkers = config.getInt(TOTAL_WORKER_NUM);

        String spoutName = "runningQueueSpout";
        String boltName = "parserBolt";

        builder.setSpout(spoutName, spout, 1);
        builder.setBolt(boltName, bolt, numOfParserTasks).shuffleGrouping(spoutName);

        StormTopology topology = builder.createTopology();

        backtype.storm.Config stormConf = new backtype.storm.Config();
        stormConf.setNumWorkers(numOfTotalWorkers);
        stormConf.put(stormConf.TOPOLOGY_DEBUG, true);

        String topoName = config.getString(TOPOLOGY_NAME);
        Boolean local = config.getBoolean(LOCAL_MODE);
        try {
            if (!local) {
                StormSubmitter.submitTopology(topoName, stormConf, topology);
            } else {
                //local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topoName, stormConf, topology);
            }
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        }

    }
}
