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

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;

/**
 * service stub to get metadata from remote metadata service
 */
public interface IMetadataServiceClient extends Closeable, Serializable {

    // user metadta
    void addStreamingCluster(StreamingCluster cluster);
    List<StreamingCluster> listClusters();
    
    List<Topology> listTopologies();
    void addTopology(Topology t);

    void addPolicy(PolicyDefinition policy);
    List<PolicyDefinition> listPolicies();

    void addStreamDefinition(StreamDefinition streamDef);
    List<StreamDefinition> listStreams();

    void addDataSource(Kafka2TupleMetadata k2t);
    List<Kafka2TupleMetadata> listDataSources();

    void addPublishment(Publishment pub);
    List<Publishment> listPublishment();

    // monitor metadata
    List<SpoutSpec> listSpoutMetadata();

    ScheduleState getVersionedSpec();
    ScheduleState getVersionedSpec(String version);
    void addScheduleState(ScheduleState state);
    
    void clear();
    
    // for topology mgmt
}
