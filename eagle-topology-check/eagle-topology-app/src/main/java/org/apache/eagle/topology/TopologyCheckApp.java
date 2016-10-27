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

package org.apache.eagle.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.topology.storm.HealthCheckParseBolt;
import org.apache.eagle.topology.storm.TopologyCheckAppSpout;
import org.apache.eagle.topology.storm.TopologyDataPersistBolt;

public class TopologyCheckApp extends StormApplication {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final String SINK_TASK_NUM = "topology.numOfSinkTasks";
	private static final String TOPOLOGY_HEALTH_CHECK_STREAM = "topology_health_check_stream";
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyCheckAppConfig topologyCheckAppConfig = TopologyCheckAppConfig.getInstance(config);

        String spoutName = TopologyCheckAppConfig.TOPOLOGY_DATA_FETCH_SPOUT_NAME;
        String persistBoltName = TopologyCheckAppConfig.TOPOLOGY_ENTITY_PERSIST_BOLT_NAME;
        String parseBoltName = TopologyCheckAppConfig.PARSE_BOLT_NAME;
        String kafkaSinkBoltName = TopologyCheckAppConfig.KAFKA_SINK_BOLT_NAME;
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);
        
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(
            spoutName,
            new TopologyCheckAppSpout(topologyCheckAppConfig),
            topologyCheckAppConfig.dataExtractorConfig.numDataFetcherSpout
        ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numDataFetcherSpout);

        topologyBuilder.setBolt(
            persistBoltName,
            new TopologyDataPersistBolt(topologyCheckAppConfig),
            topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt
        ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt).shuffleGrouping(spoutName);        
       
        topologyBuilder.setBolt(
    		parseBoltName,
    		new HealthCheckParseBolt(),
    		topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt).shuffleGrouping(persistBoltName);      
        
        StormStreamSink<?> sinkBolt = environment.getStreamSink(TOPOLOGY_HEALTH_CHECK_STREAM,config);
        topologyBuilder.setBolt(kafkaSinkBoltName, sinkBolt, numOfSinkTasks).setNumTasks(numOfSinkTasks);
        
        return topologyBuilder.createTopology();
    }
}
