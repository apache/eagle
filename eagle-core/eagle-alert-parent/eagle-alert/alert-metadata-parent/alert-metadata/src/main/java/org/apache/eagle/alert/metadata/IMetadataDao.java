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
package org.apache.eagle.alert.metadata;

import java.io.Closeable;
import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.metadata.resource.Models;
import org.apache.eagle.alert.metadata.resource.OpResult;

public interface IMetadataDao extends Closeable {

    List<Topology> listTopologies();

    OpResult addTopology(Topology t);

    OpResult removeTopology(String topologyName);

    List<StreamingCluster> listClusters();

    OpResult addCluster(StreamingCluster cluster);

    OpResult removeCluster(String clusterId);

    List<StreamDefinition> listStreams();

    OpResult createStream(StreamDefinition stream);

    OpResult removeStream(String streamId);

    List<Kafka2TupleMetadata> listDataSources();

    OpResult addDataSource(Kafka2TupleMetadata dataSource);

    OpResult removeDataSource(String datasourceId);

    List<PolicyDefinition> listPolicies();

    OpResult addPolicy(PolicyDefinition policy);

    OpResult removePolicy(String policyId);

    List<Publishment> listPublishment();

    OpResult addPublishment(Publishment publishment);

    OpResult removePublishment(String pubId);

    List<PublishmentType> listPublishmentType();

    OpResult addPublishmentType(PublishmentType publishmentType);

    OpResult removePublishmentType(String pubType);

    ScheduleState getScheduleState(String versionId);

    ScheduleState getScheduleState();

    OpResult addScheduleState(ScheduleState state);

    List<PolicyAssignment> listAssignments();

    OpResult addAssignment(PolicyAssignment assignment);

    // APIs for test friendly
    OpResult clear();

    Models export();

    OpResult importModels(Models models);

}
