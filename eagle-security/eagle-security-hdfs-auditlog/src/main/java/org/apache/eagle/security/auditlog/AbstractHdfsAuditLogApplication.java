/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.impl.storm.partition.CustomPartitionGrouping;
import org.apache.eagle.partition.DataDistributionDao;
import org.apache.eagle.partition.PartitionAlgorithm;
import org.apache.eagle.partition.PartitionStrategy;
import org.apache.eagle.partition.PartitionStrategyImpl;
import org.apache.eagle.security.partition.DataDistributionDaoImpl;
import org.apache.eagle.security.partition.GreedyPartitionAlgorithm;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import storm.kafka.StringScheme;

/**
 * Since 8/10/16.
 */
public abstract class AbstractHdfsAuditLogApplication extends StormApplication {
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String SENSITIVITY_JOIN_TASK_NUM = "topology.numOfSensitivityJoinTasks";
    public final static String IPZONE_JOIN_TASK_NUM = "topology.numOfIPZoneJoinTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutProvider provider = new KafkaSpoutProvider();
        IRichSpout spout = provider.getSpout(config);

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfSensitivityJoinTasks = config.getInt(SENSITIVITY_JOIN_TASK_NUM);
        int numOfIPZoneJoinTasks = config.getInt(IPZONE_JOIN_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks);


        HdfsAuditLogParserBolt parserBolt = new HdfsAuditLogParserBolt();
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", parserBolt, numOfParserTasks);

        Boolean useDefaultPartition = !config.hasPath("eagleProps.useDefaultPartition") || config.getBoolean("eagleProps.useDefaultPartition");
        if(useDefaultPartition){
            boltDeclarer.fieldsGrouping("ingest", new Fields(StringScheme.STRING_SCHEME_KEY));
        }else{
            boltDeclarer.customGrouping("ingest", new CustomPartitionGrouping(createStrategy(config)));
        }

        FileSensitivityDataJoinBolt sensitivityDataJoinBolt = new FileSensitivityDataJoinBolt(config);
        BoltDeclarer sensitivityDataJoinBoltDeclarer = builder.setBolt("sensitivityJoin", sensitivityDataJoinBolt, numOfSensitivityJoinTasks);
        sensitivityDataJoinBoltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));

        IPZoneDataJoinBolt ipZoneDataJoinBolt = new IPZoneDataJoinBolt(config);
        BoltDeclarer ipZoneDataJoinBoltDeclarer = builder.setBolt("ipZoneJoin", ipZoneDataJoinBolt, numOfIPZoneJoinTasks);
        ipZoneDataJoinBoltDeclarer.fieldsGrouping("sensitivityJoin", new Fields("user"));

        StormStreamSink sinkBolt = environment.getStreamSink("hdfs_audit_log_stream",config);
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, numOfSinkTasks);
        kafkaBoltDeclarer.fieldsGrouping("ipZoneJoin", new Fields("user"));
        return builder.createTopology();


    }

    public abstract BaseRichBolt getParserBolt();
    public abstract String getSinkStreamName();

    public static PartitionStrategy createStrategy(Config config) {
        // TODO: Refactor configuration structure to avoid repeated config processing configure ~ hao
        String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        Integer port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        String username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
        String password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);
        String topic = config.getString("dataSourceConfig.topic");
        DataDistributionDao dao = new DataDistributionDaoImpl(host, port, username, password, topic);
        PartitionAlgorithm algorithm = new GreedyPartitionAlgorithm();
        String key1 = EagleConfigConstants.EAGLE_PROPS + ".partitionRefreshIntervalInMin";
        Integer partitionRefreshIntervalInMin = config.hasPath(key1) ? config.getInt(key1) : 60;
        String key2 = EagleConfigConstants.EAGLE_PROPS + ".kafkaStatisticRangeInMin";
        Integer kafkaStatisticRangeInMin =  config.hasPath(key2) ? config.getInt(key2) : 60;
        PartitionStrategy strategy = new PartitionStrategyImpl(dao, algorithm, partitionRefreshIntervalInMin * DateUtils.MILLIS_PER_MINUTE, kafkaStatisticRangeInMin * DateUtils.MILLIS_PER_MINUTE);
        return strategy;
    }


}
