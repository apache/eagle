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

package org.apache.eagle.state;

/**
 * constants for policy state management
 */
public class ExecutorStateConstants {
    public final static String POLICY_STATE_SNAPSHOT_SERVICE_ENDPOINT_NAME = "PolicyStateSnapshotService";
    public final static String POLICY_STATE_DELTA_EVENT_ID_RANGE_SERVICE_ENDPOINT_NAME = "PolicyStateDeltaEventIDRangeService";

    // zookeeper access constants
    public final static String ZOOKEEPER_RETRY_TIMES_PROPERTY = "eagleProps.executorState.zookeeper.retry.times";
    public final static int ZOOKEEPER_RETRY_TIMES_DEFAULT = 3;
    public final static String ZOOKEEPER_RETRY_SLEEP_BETWEEN_RETRIES_PROPERTY = "eagleProps.executorState.zookeeper.sleepMsBetweenRetries";
    public final static int ZOOKEEPER_RETRY_SLEEP_BETWEEN_RETRIES_DEFAULT = 2000;
    public final static String ZOOKEEPER_ZKPATH_PROPERTY = "eagleProps.executorState.zookeeper.zkPath";
    public final static String ZOOKEEPER_ZKPATH_DEFAULT = "/brokers";
}
