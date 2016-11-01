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
package org.apache.eagle.alert.engine.integration;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.service.IMetadataServiceClient;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("serial")
public class MockMetadataServiceClient implements IMetadataServiceClient {

    @Override
    public List<SpoutSpec> listSpoutMetadata() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public ScheduleState getVersionedSpec(String version) {
        return null;
    }

    @Override
    public List<StreamingCluster> listClusters() {
        return null;
    }

    @Override
    public List<PolicyDefinition> listPolicies() {
        return null;
    }

    @Override
    public List<StreamDefinition> listStreams() {
        return null;
    }

    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        return null;
    }

    @Override
    public List<Publishment> listPublishment() {
        return null;
    }

    @Override
    public ScheduleState getVersionedSpec() {
        return null;
    }

    @Override
    public void addScheduleState(ScheduleState state) {

    }

    @Override
    public List<Topology> listTopologies() {
        return null;
    }

    @Override
    public void addStreamingCluster(StreamingCluster cluster) {

    }

    @Override
    public void addStreamingClusters(List<StreamingCluster> clusters) {

    }

    @Override
    public void addTopology(Topology t) {

    }

    @Override
    public void addTopologies(List<Topology> topologies) {

    }

    @Override
    public void addPolicy(PolicyDefinition policy) {

    }

    @Override
    public void addPolicies(List<PolicyDefinition> policies) {

    }

    @Override
    public void addStreamDefinition(StreamDefinition streamDef) {

    }

    @Override
    public void addStreamDefinitions(List<StreamDefinition> streamDefs) {

    }

    @Override
    public void addDataSource(Kafka2TupleMetadata k2t) {

    }

    @Override
    public void addDataSources(List<Kafka2TupleMetadata> k2ts) {

    }

    @Override
    public void addPublishment(Publishment pub) {

    }

    @Override
    public void addPublishments(List<Publishment> pubs) {

    }

    @Override
    public void clear() {

    }

    @Override
    public List<AlertPublishEvent> listAlertPublishEvent() {
        return null;
    }

    @Override
    public void addAlertPublishEvent(AlertPublishEvent event) {

    }

    @Override
    public void addAlertPublishEvents(List<AlertPublishEvent> events) {

    }
}