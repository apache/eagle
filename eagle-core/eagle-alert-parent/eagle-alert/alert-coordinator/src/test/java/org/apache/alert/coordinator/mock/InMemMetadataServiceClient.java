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
package org.apache.alert.coordinator.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.service.IMetadataServiceClient;

/**
 * @since May 5, 2016
 *
 */
@SuppressWarnings("serial")
public class InMemMetadataServiceClient implements IMetadataServiceClient {

    private List<StreamingCluster> clusters = new ArrayList<StreamingCluster>();
    private List<Topology> topologies = new ArrayList<Topology>();
    private List<PolicyDefinition> policies = new ArrayList<PolicyDefinition>();
    private List<StreamDefinition> definitions = new ArrayList<StreamDefinition>();
    private List<Kafka2TupleMetadata> datasources = new ArrayList<Kafka2TupleMetadata>();

    private SortedMap<String, ScheduleState> scheduleStates = new TreeMap<String, ScheduleState>();
    private List<SpoutSpec> spoutSpecs = new ArrayList<SpoutSpec>();
    private List<Publishment> publishmetns = new ArrayList<Publishment>();

    @Override
    public void close() throws IOException {
    }

    @Override
    public List<StreamingCluster> listClusters() {
        return clusters;
    }

    @Override
    public List<Topology> listTopologies() {
        return topologies;
    }

    @Override
    public List<PolicyDefinition> listPolicies() {
        return policies;
    }

    @Override
    public List<StreamDefinition> listStreams() {
        return definitions;
    }

    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        return datasources;
    }

    @Override
    public List<Publishment> listPublishment() {
        return publishmetns;
    }

    @Override
    public List<SpoutSpec> listSpoutMetadata() {
        return spoutSpecs;
    }

    @Override
    public ScheduleState getVersionedSpec() {
        Iterator<Entry<String, ScheduleState>> it = scheduleStates.entrySet().iterator();
        if (it.hasNext()) {
            return it.next().getValue();
        }
        return null;
    }

    @Override
    public ScheduleState getVersionedSpec(String version) {
        return scheduleStates.get(version);
    }

    @Override
    public void addScheduleState(ScheduleState state) {
        scheduleStates.put(state.getVersion(), state);
    }

    @Override
    public void addStreamingCluster(StreamingCluster cluster) {
        clusters.add(cluster);
    }

    @Override
    public void addStreamingClusters(List<StreamingCluster> clusters) {
        this.clusters.addAll(clusters);
    }

    @Override
    public void addTopology(Topology t) {
        topologies.add(t);
    }

    @Override
    public void addTopologies(List<Topology> topologies) {
        this.topologies.addAll(topologies);
    }

    @Override
    public void addPolicy(PolicyDefinition policy) {
        policies.add(policy);
    }

    @Override
    public void addPolicies(List<PolicyDefinition> policies) {
        this.policies.addAll(policies);
    }

    @Override
    public void addStreamDefinition(StreamDefinition streamDef) {
        definitions.add(streamDef);
    }

    @Override
    public void addStreamDefinitions(List<StreamDefinition> streamDefs) {
        this.definitions.addAll(streamDefs);
    }

    @Override
    public void addDataSource(Kafka2TupleMetadata k2t) {
        datasources.add(k2t);
    }

    @Override
    public void addDataSources(List<Kafka2TupleMetadata> k2ts) {
        this.datasources.addAll(k2ts);
    }

    @Override
    public void addPublishment(Publishment pub) {
        publishmetns.add(pub);
    }

    @Override
    public void addPublishments(List<Publishment> pubs) {
        this.publishmetns.addAll(pubs);
    }

    @Override
    public void clear() {
        // do nothing
    }

}
