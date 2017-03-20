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
import org.apache.eagle.app.messaging.StormStreamSink;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import org.apache.eagle.topology.extractor.system.SystemCheckPersistBolt;
import org.apache.eagle.topology.storm.HealthCheckParseBolt;
import org.apache.eagle.topology.storm.TopologyCheckAppSpout;
import org.apache.eagle.topology.storm.TopologyDataPersistBolt;

public class TopologyCheckApp extends StormApplication {

    private static final String TOPOLOGY_HEALTH_CHECK_STREAM = "topology_health_check_stream";
    private static final String SYSTEM_COLLECTOR_CONFIG_PREFIX = "dataSourceConfig.system";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyCheckAppConfig topologyCheckAppConfig = TopologyCheckAppConfig.newInstance(config);

        String spoutName = TopologyCheckAppConfig.TOPOLOGY_DATA_FETCH_SPOUT_NAME;
        String systemSpoutName = TopologyCheckAppConfig.SYSTEM_DATA_FETCH_SPOUT_NAME;
        String systemPersistBoltName = TopologyCheckAppConfig.SYSTEM_ENTITY_PERSIST_BOLT_NAME;
        String persistBoltName = TopologyCheckAppConfig.TOPOLOGY_ENTITY_PERSIST_BOLT_NAME;
        String parseBoltName = TopologyCheckAppConfig.PARSE_BOLT_NAME;
        String kafkaSinkBoltName = TopologyCheckAppConfig.SINK_BOLT_NAME;

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
            topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt
        ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt).shuffleGrouping(persistBoltName);

        StormStreamSink<?> sinkBolt = environment.getStreamSink(TOPOLOGY_HEALTH_CHECK_STREAM, config);
        topologyBuilder.setBolt(
            kafkaSinkBoltName,
            sinkBolt,
            topologyCheckAppConfig.dataExtractorConfig.numKafkaSinkBolt
        ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numKafkaSinkBolt).shuffleGrouping(parseBoltName);

        // system check data collector
        if (topologyCheckAppConfig.systemConfig.systemInstanceEnable) {
            topologyBuilder.setSpout(
                systemSpoutName,
                new KafkaSpoutProvider(SYSTEM_COLLECTOR_CONFIG_PREFIX).getSpout(config),
                topologyCheckAppConfig.dataExtractorConfig.numDataFetcherSpout
            ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numDataFetcherSpout);

            // system check data persist
            topologyBuilder.setBolt(
                systemPersistBoltName,
                new SystemCheckPersistBolt(topologyCheckAppConfig),
                topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt
            ).setNumTasks(topologyCheckAppConfig.dataExtractorConfig.numEntityPersistBolt).shuffleGrouping(systemSpoutName);
        }

        return topologyBuilder.createTopology();
    }
}
