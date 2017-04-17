/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner;
import org.apache.eagle.alert.engine.runner.UnitSparkUnionTopologyRunner;

import static org.apache.eagle.alert.engine.utils.Constants.ALERT_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.AUTO_OFFSET_RESET;
import static org.apache.eagle.alert.engine.utils.Constants.BATCH_DURATION;
import static org.apache.eagle.alert.engine.utils.Constants.CHECKPOINT_PATH;
import static org.apache.eagle.alert.engine.utils.Constants.PUBLISH_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.ROUTER_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.SLIDE_DURATION_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.SPOUT_KAFKABROKERZKQUORUM;
import static org.apache.eagle.alert.engine.utils.Constants.TOPOLOGY_GROUPID;
import static org.apache.eagle.alert.engine.utils.Constants.TOPOLOGY_MULTIKAFKA;
import static org.apache.eagle.alert.engine.utils.Constants.WINDOW_DURATIONS_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.ZKCONFIG_ZKQUORUM;

public class UnitSparkUnionTopologyMain {

    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";

    public static void main(String[] args) throws InterruptedException {

        Config config;
        if (args != null && args.length == 15) {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            ImmutableMap<String, String> argsMap = builder
                .put(BATCH_DURATION, args[0])
                .put(ROUTER_TASK_NUM, args[1])
                .put(ALERT_TASK_NUM, args[2])
                .put(PUBLISH_TASK_NUM, args[3])
                .put(SLIDE_DURATION_SECOND, args[4])
                .put(WINDOW_DURATIONS_SECOND, args[5])
                .put(CHECKPOINT_PATH, args[6])
                .put(TOPOLOGY_GROUPID, args[7])
                .put(AUTO_OFFSET_RESET, args[8])
                .put(EAGLE_CORRELATION_CONTEXT, args[9])
                .put(EAGLE_CORRELATION_SERVICE_PORT, args[10])
                .put(EAGLE_CORRELATION_SERVICE_HOST, args[11])
                .put(TOPOLOGY_MULTIKAFKA, args[12])
                .put(SPOUT_KAFKABROKERZKQUORUM, args[13])
                .put(ZKCONFIG_ZKQUORUM, args[14]).build();
            config = ConfigFactory.parseMap(argsMap);
        } else {
            config = ConfigFactory.load();
        }
        boolean useMultiKafka = config.getBoolean(TOPOLOGY_MULTIKAFKA);
        if (useMultiKafka) {
            new UnitSparkUnionTopologyRunner(config).run();
        } else {
            new UnitSparkTopologyRunner(config).run();
        }

    }
}