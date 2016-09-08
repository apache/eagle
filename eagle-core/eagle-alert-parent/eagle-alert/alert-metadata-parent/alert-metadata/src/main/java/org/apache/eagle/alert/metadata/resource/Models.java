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
package org.apache.eagle.alert.metadata.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;

/**
 * This models used for metadata export/import to easy of test.
 *
 * @since May 23, 2016
 */
public class Models {
    public List<StreamingCluster> clusters = new ArrayList<StreamingCluster>();
    public List<StreamDefinition> schemas = new ArrayList<StreamDefinition>();
    public List<Kafka2TupleMetadata> datasources = new ArrayList<Kafka2TupleMetadata>();
    public List<PolicyDefinition> policies = new ArrayList<PolicyDefinition>();
    public List<Publishment> publishments = new ArrayList<Publishment>();
    public SortedMap<String, ScheduleState> scheduleStates = new TreeMap<String, ScheduleState>();
    public List<PolicyAssignment> assignments = new ArrayList<PolicyAssignment>();
    public List<Topology> topologies = new ArrayList<Topology>();
}
