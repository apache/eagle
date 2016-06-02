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

import java.util.Map;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

/**
 * @since Mar 28, 2016
 *
 */
public interface IScheduleContext {

    Map<String, Topology> getTopologies();

    Map<String, PolicyDefinition> getPolicies();

    // data source
    Map<String, Kafka2TupleMetadata> getDataSourceMetadata();

    Map<String, StreamDefinition> getStreamSchemas();

    Map<String, TopologyUsage> getTopologyUsages();

    Map<String, PolicyAssignment> getPolicyAssignments();

    Map<StreamGroup, MonitoredStream> getMonitoredStreams();
    
    Map<String, Publishment> getPublishments();

}
