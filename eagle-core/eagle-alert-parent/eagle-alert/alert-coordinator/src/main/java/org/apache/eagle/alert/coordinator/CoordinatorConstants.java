/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.coordinator;

public class CoordinatorConstants {
    public static final String CONFIG_ITEM_COORDINATOR = "coordinator";
    public static final String CONFIG_ITEM_TOPOLOGY_LOAD_UPBOUND = "topologyLoadUpbound";
    public static final String CONFIG_ITEM_BOLT_LOAD_UPBOUND = "boltLoadUpbound";
    public static final String POLICY_DEFAULT_PARALLELISM = "policyDefaultParallelism";
    public static final String BOLT_PARALLELISM = "boltParallelism";
    public static final String NUM_OF_ALERT_BOLTS_PER_TOPOLOGY = "numOfAlertBoltsPerTopology";
    public static final String POLICIES_PER_BOLT = "policiesPerBolt";
    public static final String REUSE_BOLT_IN_STREAMS = "reuseBoltInStreams";
    public static final String STREAMS_PER_BOLT = "streamsPerBolt";
}
