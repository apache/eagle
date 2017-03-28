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

package org.apache.eagle.alert.engine.utils;

public class Constants {
    public static final String ALERTBOLTNAME_PREFIX = "alertBolt";
    public static final String TOPOLOGY_ID = "topologyId";
    public static final int DEFAULT_BATCH_DURATION_SECOND = 2;
    public static final String alertPublishBoltName = "alertPublishBolt";
    //spark config
    public static final String BATCH_DURATION = "topology.batchDuration";
    public static final String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    public static final String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    public static final String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    public static final String SLIDE_DURATION_SECOND = "topology.slideDurations";
    public static final String WINDOW_DURATIONS_SECOND = "topology.windowDurations";
    public static final String CHECKPOINT_PATH = "topology.checkpointPath";
    public static final String TOPOLOGY_GROUPID = "topology.groupId";
    public static final String AUTO_OFFSET_RESET = "topology.offsetreset";
    public static final String SPOUT_KAFKABROKERZKQUORUM = "spout.kafkaBrokerZkQuorum";
    public static final String ZKCONFIG_ZKQUORUM = "zkConfig.zkQuorum";
    public static final String TOPOLOGY_MULTIKAFKA = "topology.multikafka";
}
