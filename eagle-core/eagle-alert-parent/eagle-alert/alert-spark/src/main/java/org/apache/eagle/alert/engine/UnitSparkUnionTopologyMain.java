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
import org.apache.eagle.alert.engine.runner.UnitSparkUnionTopologyRunner;


public class UnitSparkUnionTopologyMain {

    private static final String BATCH_DURATION = "topology.batchDuration";
    private static final String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private static final String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private static final String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    private static final String SLIDE_DURATION_SECOND = "topology.slideDurations";
    private static final String WINDOW_DURATIONS_SECOND = "topology.windowDurations";
    private static final String CHECKPOINT_PATH = "topology.checkpointPath";
    private static final String TOPOLOGY_GROUPID = "topology.groupId";
    private static final String AUTO_OFFSET_RESET = "topology.offsetreset";
    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";

    public static void main(String[] args) throws InterruptedException {

        Config config;
        if (args != null && args.length == 12) {
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
                .put(EAGLE_CORRELATION_SERVICE_HOST, args[11]).build();
            config = ConfigFactory.parseMap(argsMap);
        } else {
            config = ConfigFactory.load();
        }

        new UnitSparkUnionTopologyRunner(config).run();
    }
}