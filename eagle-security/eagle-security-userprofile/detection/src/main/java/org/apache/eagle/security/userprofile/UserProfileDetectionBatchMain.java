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
package org.apache.eagle.security.userprofile;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.apache.eagle.datastream.*;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UserProfileDetectionBatchMain {
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileDetectionBatchMain.class);

    public static void main(String[] args) throws Exception{
        new ConfigOptionParser().load(args);
        System.setProperty("config.trace", "loads");
        Config config = ConfigFactory.load();

        LOG.info("Config class: " + config.getClass().getCanonicalName());

        if(LOG.isDebugEnabled()) LOG.debug("Config content:"+config.root().render(ConfigRenderOptions.concise()));

        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
        env.newSource(new KafkaSourcedSpoutProvider().getSpout(config)).renameOutputFields(1)
                .flatMap(new UserActivityPartitionExecutor())
                .alertWithConsumer(UserProfileDetectionConstants.USER_ACTIVITY_AGGREGATION_STREAM,
                        UserProfileDetectionConstants.USER_PROFILE_ANOMALY_DETECTION_EXECUTOR);
        env.execute();
    }

    public static class UserActivityPartitionExecutor extends JavaStormStreamExecutor2<String, Map> {
        private final static Logger LOG = LoggerFactory.getLogger(UserActivityPartitionExecutor.class);
        @Override
        public void prepareConfig(Config config) {
            // do nothing
        }

        @Override
        public void init() {
            // do nothing
        }

        @Override
        public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, Map>> outputCollector){
            if(input.size()>0){
                Object obj = input.get(0);
                if(obj instanceof UserActivityAggModelEntity){
                    UserActivityAggModelEntity entity = (UserActivityAggModelEntity) obj;
                    String user = entity.getTags() == null? null: entity.getTags().get(UserProfileConstants.USER_TAG);
                    outputCollector.collect(new Tuple2(user,entity));
                }else{
                    LOG.warn(String.format("%s is not instance of UserActivityAggModelEntity, skip",obj));
                }
            }
        }
    }
}