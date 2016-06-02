/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.spark.history.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.apache.eagle.jpm.spark.history.config.SparkHistoryCrawlConfig;

/**
 * Created by jnwang on 2016/5/6.
 */
public class SparkHistoryTopology {

    private SparkHistoryCrawlConfig config;

    public SparkHistoryTopology(SparkHistoryCrawlConfig config){
        this.config = config;
    }

    public TopologyBuilder getBuilder(){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("getJobs", new FinishedSparkJobSpout(config), 1).setNumTasks(1);
        builder.setBolt("parseJobs", new SparkJobParseBolt(config), 8).setNumTasks(8).shuffleGrouping("getJobs");
        return builder;
    }


    public static void main(String[] args){

        SparkHistoryCrawlConfig crawlConfig = new SparkHistoryCrawlConfig();

        Config conf = new Config();
        conf.setNumWorkers(crawlConfig.stormConfig.workerNo);
        conf.setMessageTimeoutSecs(crawlConfig.stormConfig.timeoutSec);
        //conf.setMaxSpoutPending(crawlConfig.stormConfig.spoutPending);
        conf.put(Config.TOPOLOGY_DEBUG, true);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(crawlConfig.stormConfig.topologyName, conf, new SparkHistoryTopology(crawlConfig).getBuilder().createTopology());

    }
}
