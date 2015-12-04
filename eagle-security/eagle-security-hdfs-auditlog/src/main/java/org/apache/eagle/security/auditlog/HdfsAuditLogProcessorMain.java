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
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.StormExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HdfsAuditLogProcessorMain {

	public static void main(String[] args) throws Exception{
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm(args);
        String deserClsName = env.getConfig().getString("dataSourceConfig.deserializerClass");
        final KafkaSourcedSpoutScheme scheme = new KafkaSourcedSpoutScheme(deserClsName, env.getConfig()) {
                @Override
                public List<Object> deserialize(byte[] ser) {
                        Object tmp = deserializer.deserialize(ser);
                        Map<String, Object> map = (Map<String, Object>)tmp;
                        if(tmp == null) return null;
                        return Arrays.asList(map.get("user"), tmp);
                }
        };
        KafkaSourcedSpoutProvider provider = new KafkaSourcedSpoutProvider() {
                public SchemeAsMultiScheme getStreamScheme(String deserClsName, Config context) {
                        return new SchemeAsMultiScheme(scheme);
                }
        };
        env.from(provider.getSpout(env.getConfig())).renameOutputFields(2).name("kafkaMsgConsumer").groupBy(Arrays.asList(0))
                .flatMap(new FileSensitivityDataJoinExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new IPZoneDataJoinExecutor())
                .alertWithConsumer("hdfsAuditLogEventStream", "hdfsAuditLogAlertExecutor");
        env.execute();
	}
}