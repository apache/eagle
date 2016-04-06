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

package org.apache.eagle.service.application;


public class AppManagerConstants {
    public final static String SITE_TAG = "site";
    public final static String APPLICATION_TAG = "application";
    public final static String COMMAND_TYPE_TAG = "operation";
    public final static String COMMAND_ID_TAG = "uuid";
    public final static String TOPOLOGY_TAG = "topology";
    public final static String NAME_TAG = "fullName";

    public final static String EAGLE_CLUSTER_STORM = "storm";
    public final static String EAGLE_CLUSTER_SPARK = "spark";
    public final static String EAGLE_CLUSTER_TYPE = "type";

    public final static String EAGLE_CONFIG_FILE = "eagle-scheduler.conf";
    public final static String EAGLE_SERVICE_CONFIG = "eagle.service";
    public final static String EAGLE_SCHEDULER_CONFIG = "eagle.scheduler";
    public final static String EAGLE_STORM_JARFILE = "storm.jar";
    public final static String EAGLE_STORM_NIMBUS = "nimbus.host";
    public final static String EAGLE_STORM_NIMBUS_PORT = "nimbus.thrift.port";
    //public final static String EAGLE_STORM_THRIFT_TRANSPORT_PLUGIN = "storm.thrift.transport";
    public final static String SCHEDULE_SYSTEM = "scheduleSystem";
    public final static String SCHEDULE_INTERVAL = "scheduleInterval";
    public final static String SCHEDULE_NUM_WORKERS = "scheduleNumWorkers";
    public final static String SERVICE_TIMEOUT = "serviceWaitTimeOut";
}
