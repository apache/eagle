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
package org.apache.eagle.security.auditlog;

import backtype.storm.spout.SchemeAsMultiScheme;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.apache.eagle.datastream.ExecutionEnvironmentFactory;
import org.apache.eagle.datastream.StormExecutionEnvironment;
import org.apache.eagle.partition.DataDistributionDao;
import org.apache.eagle.partition.PartitionAlgorithm;
import org.apache.eagle.partition.PartitionStrategy;
import org.apache.eagle.partition.PartitionStrategyImpl;
import org.apache.eagle.security.partition.DataDistributionDaoImpl;
import org.apache.eagle.security.partition.GreedyPartitionAlgorithm;
import org.apache.eagle.datastream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HdfsAuditLogProcessorMain {
	private static final Logger LOG = LoggerFactory.getLogger(HdfsAuditLogProcessorMain.class);

    public static PartitionStrategy createStrategy(Config config) {
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

    public static KafkaSourcedSpoutProvider createProvider(Config config) {
         String deserClsName = config.getString("dataSourceConfig.deserializerClass");
         final KafkaSourcedSpoutScheme scheme = new KafkaSourcedSpoutScheme(deserClsName, config) {
                 @Override
                 public List<Object> deserialize(byte[] ser) {
                         Object tmp = deserializer.deserialize(ser);
                         Map<String, Object> map = (Map<String, Object>)tmp;
                         if(tmp == null) return null;
                         return Arrays.asList(map.get("user"), tmp);
                 }
         };
         KafkaSourcedSpoutProvider provider = new KafkaSourcedSpoutProvider() {
                 @Override
                 public SchemeAsMultiScheme getStreamScheme(String deserClsName, Config context) {
                         return new SchemeAsMultiScheme(scheme);
                  }
         };
         return provider;
    }

    public static void execWithDefaultPartition(Config config, StormExecutionEnvironment env, KafkaSourcedSpoutProvider provider) {
        StreamProducer source = env.newSource(provider.getSpout(config)).renameOutputFields(2).withName("kafkaMsgConsumer").groupBy(Arrays.asList(0));
        StreamProducer reassembler = source.flatMap(new HdfsUserCommandReassembler()).groupBy(Arrays.asList(0));
        source.streamUnion(reassembler)
              .flatMap(new FileSensitivityDataJoinExecutor()).groupBy(Arrays.asList(0))
              .flatMap(new IPZoneDataJoinExecutor())
              .alertWithConsumer("hdfsAuditLogEventStream", "hdfsAuditLogAlertExecutor");
        env.execute();
    }

    public static void execWithBalancedPartition(Config config, StormExecutionEnvironment env, KafkaSourcedSpoutProvider provider) {
        PartitionStrategy strategy = createStrategy(config);
        StreamProducer source = env.newSource(provider.getSpout(config)).renameOutputFields(2).withName("kafkaMsgConsumer").customGroupBy(strategy);
        StreamProducer reassembler = source.flatMap(new HdfsUserCommandReassembler()).groupBy(Arrays.asList(0));
        source.streamUnion(reassembler)
              .flatMap(new FileSensitivityDataJoinExecutor()).groupBy(Arrays.asList(0))
              .flatMap(new IPZoneDataJoinExecutor())
              .alertWithConsumer("hdfsAuditLogEventStream", "hdfsAuditLogAlertExecutor");
        env.execute();
    }

	public static void main(String[] args) throws Exception{
        Config config = new ConfigOptionParser().load(args);
        LOG.info("Config class: " + config.getClass().getCanonicalName());
        if(LOG.isDebugEnabled()) LOG.debug("Config content:"+config.root().render(ConfigRenderOptions.concise()));

        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
        KafkaSourcedSpoutProvider provider = createProvider(config);
        Boolean balancePartition = config.hasPath("eagleProps.balancePartitionEnabled") ? config.getBoolean("eagleProps.balancePartitionEnabled") : false;
        if (balancePartition) {
            execWithBalancedPartition(config, env, provider);
        }
        else {
            execWithDefaultPartition(config, env, provider);
        }
	}
}