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
package org.apache.eagle.alert.coordinator.provider;

import java.util.HashMap;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.StreamGroup;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

/**
 * @since Mar 28, 2016
 *
 */
public class InMemScheduleConext implements IScheduleContext {

    private Map<String, Topology> topologies = new HashMap<String, Topology>();
    private Map<String, TopologyUsage> usages = new HashMap<String, TopologyUsage>();
    private Map<String, PolicyDefinition> policies = new HashMap<String, PolicyDefinition>();
    private Map<String, Kafka2TupleMetadata> datasources = new HashMap<String, Kafka2TupleMetadata>();
    private Map<String, PolicyAssignment> policyAssignments = new HashMap<String, PolicyAssignment>();
    private Map<String, StreamDefinition> schemas = new HashMap<String, StreamDefinition>();
    private Map<StreamGroup, MonitoredStream> monitoredStreams = new HashMap<StreamGroup, MonitoredStream>();
    private Map<String, Publishment> publishments = new HashMap<String, Publishment>();

    public InMemScheduleConext() {
    }

    public InMemScheduleConext(IScheduleContext context) {
        this.topologies = new HashMap<String, Topology>(context.getTopologies());
        this.usages = new HashMap<String, TopologyUsage>(context.getTopologyUsages());
        this.policies = new HashMap<String, PolicyDefinition>(context.getPolicies());
        this.datasources = new HashMap<String, Kafka2TupleMetadata>(context.getDataSourceMetadata());
        this.policyAssignments = new HashMap<String, PolicyAssignment>(context.getPolicyAssignments());
        this.schemas = new HashMap<String, StreamDefinition>(context.getStreamSchemas());
        this.monitoredStreams = new HashMap<StreamGroup, MonitoredStream>(context.getMonitoredStreams());
        this.publishments = new HashMap<String, Publishment>(context.getPublishments());
    }

    public InMemScheduleConext(Map<String, Topology> topologies2, Map<String, PolicyAssignment> assignments,
            Map<String, Kafka2TupleMetadata> kafkaSources, Map<String, PolicyDefinition> policies2,
            Map<String, Publishment> publishments2, Map<String, StreamDefinition> streamDefinitions,
            Map<StreamGroup, MonitoredStream> monitoredStreamMap, Map<String, TopologyUsage> usages2) {
        this.topologies = topologies2;
        this.policyAssignments = assignments;
        this.datasources = kafkaSources;
        this.policies = policies2;
        this.publishments = publishments2;
        this.schemas = streamDefinitions;
        this.monitoredStreams = monitoredStreamMap;
        this.usages = usages2;
    }

    public Map<String, Topology> getTopologies() {
        return topologies;
    }

    public void addTopology(Topology topo) {
        topologies.put(topo.getName(), topo);
    }

    public Map<String, TopologyUsage> getTopologyUsages() {
        return usages;
    }

    public void addTopologyUsages(TopologyUsage usage) {
        usages.put(usage.getTopoName(), usage);
    }

    public Map<String, PolicyDefinition> getPolicies() {
        return policies;
    }

    public void addPoilcy(PolicyDefinition pd) {
        this.policies.put(pd.getName(), pd);
    }

    public Map<String, Kafka2TupleMetadata> getDatasources() {
        return datasources;
    }

    public void setDatasources(Map<String, Kafka2TupleMetadata> datasources) {
        this.datasources = datasources;
    }

    public void addDataSource(Kafka2TupleMetadata dataSource) {
        this.datasources.put(dataSource.getName(), dataSource);
    }

    @Override
    public Map<String, Kafka2TupleMetadata> getDataSourceMetadata() {
        return datasources;
    }

    public void setPolicyOrderedTopologies(Map<String, PolicyAssignment> policyAssignments) {
        this.policyAssignments = policyAssignments;
    }

    public Map<String, PolicyAssignment> getPolicyAssignments() {
        return this.policyAssignments;
    }

    @Override
    public Map<String, StreamDefinition> getStreamSchemas() {
        return schemas;
    }

    public void addSchema(StreamDefinition schema) {
        this.schemas.put(schema.getStreamId(), schema);
    }

    public void setStreamSchemas(Map<String, StreamDefinition> schemas) {
        this.schemas = schemas;
    }

    @Override
    public Map<StreamGroup, MonitoredStream> getMonitoredStreams() {
        return monitoredStreams;
    }

    @Override
    public Map<String, Publishment> getPublishments() {
        return publishments;
    }

}
