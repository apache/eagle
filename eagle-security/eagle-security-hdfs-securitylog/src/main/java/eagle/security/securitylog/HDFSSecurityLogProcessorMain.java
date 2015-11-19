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
 *
 */

package eagle.security.securitylog;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.apache.eagle.datastream.ExecutionEnvironmentFactory;
import org.apache.eagle.datastream.StormExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSecurityLogProcessorMain {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSSecurityLogProcessorMain.class);

    public static void main(String[] args) throws Exception{
        new ConfigOptionParser().load(args);
        System.setProperty("config.trace", "loads");
        Config config = ConfigFactory.load();

        LOG.info("Config class: " + config.getClass().getCanonicalName());

        if(LOG.isDebugEnabled()) LOG.debug("Config content:"+config.root().render(ConfigRenderOptions.concise()));

        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);

        env.newSource(new KafkaSourcedSpoutProvider().getSpout(config)).renameOutputFields(1).withName("kafkaMsgConsumer")
                .alertWithConsumer("hdfsSecurityLogEventStream", "hdfsSecurityLogAlertExecutor");
        env.execute();
    }
}
