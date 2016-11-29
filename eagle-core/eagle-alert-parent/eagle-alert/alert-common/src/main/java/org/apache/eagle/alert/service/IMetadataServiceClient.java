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

package org.apache.eagle.alert.service;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;

/**
 * service stub to get metadata from remote metadata service.
 */
public interface IMetadataServiceClient extends Closeable, Serializable {

    // user metadta
    void addStreamingCluster(StreamingCluster cluster);

    void addStreamingClusters(List<StreamingCluster> clusters);

    List<StreamingCluster> listClusters();

    List<Topology> listTopologies();

    void addTopology(Topology t);

    void addTopologies(List<Topology> topologies);

    void addPolicy(PolicyDefinition policy);

    void addPolicies(List<PolicyDefinition> policies);

    List<PolicyDefinition> listPolicies();

    void addStreamDefinition(StreamDefinition streamDef);

    void addStreamDefinitions(List<StreamDefinition> streamDefs);

    List<StreamDefinition> listStreams();

    void addDataSource(Kafka2TupleMetadata k2t);

    void addDataSources(List<Kafka2TupleMetadata> k2ts);

    List<Kafka2TupleMetadata> listDataSources();

    void addPublishment(Publishment pub);

    void addPublishments(List<Publishment> pubs);

    List<Publishment> listPublishment();

    // monitor metadata
    List<SpoutSpec> listSpoutMetadata();

    ScheduleState getVersionedSpec();

    ScheduleState getVersionedSpec(String version);

    void addScheduleState(ScheduleState state);

    void clear();

    void clearScheduleState(int maxCapacity);

    // for topology mgmt

    // for alert event
    List<AlertPublishEvent> listAlertPublishEvent();

    void addAlertPublishEvent(AlertPublishEvent event);

    void addAlertPublishEvents(List<AlertPublishEvent> events);

}
