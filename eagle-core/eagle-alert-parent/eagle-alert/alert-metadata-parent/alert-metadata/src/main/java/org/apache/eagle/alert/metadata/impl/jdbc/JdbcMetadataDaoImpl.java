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
package org.apache.eagle.alert.metadata.impl.jdbc;

import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.apache.eagle.alert.metadata.resource.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.Models;

/**
 * @since May 26, 2016
 *
 */
public class JdbcMetadataDaoImpl implements IMetadataDao {

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listTopologies()
     */
    @Override
    public List<Topology> listTopologies() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addTopology(org.apache.eagle.alert.coordination.model.internal.Topology)
     */
    @Override
    public OpResult addTopology(Topology t) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removeTopology(java.lang.String)
     */
    @Override
    public OpResult removeTopology(String topologyName) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listClusters()
     */
    @Override
    public List<StreamingCluster> listClusters() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addCluster(org.apache.eagle.alert.engine.coordinator.StreamingCluster)
     */
    @Override
    public OpResult addCluster(StreamingCluster cluster) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removeCluster(java.lang.String)
     */
    @Override
    public OpResult removeCluster(String clusterId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listStreams()
     */
    @Override
    public List<StreamDefinition> listStreams() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#createStream(org.apache.eagle.alert.engine.coordinator.StreamDefinition)
     */
    @Override
    public OpResult createStream(StreamDefinition stream) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removeStream(java.lang.String)
     */
    @Override
    public OpResult removeStream(String streamId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listDataSources()
     */
    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addDataSource(org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata)
     */
    @Override
    public OpResult addDataSource(Kafka2TupleMetadata dataSource) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removeDataSource(java.lang.String)
     */
    @Override
    public OpResult removeDataSource(String datasourceId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listPolicies()
     */
    @Override
    public List<PolicyDefinition> listPolicies() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addPolicy(org.apache.eagle.alert.engine.coordinator.PolicyDefinition)
     */
    @Override
    public OpResult addPolicy(PolicyDefinition policy) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removePolicy(java.lang.String)
     */
    @Override
    public OpResult removePolicy(String policyId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listPublishment()
     */
    @Override
    public List<Publishment> listPublishment() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addPublishment(org.apache.eagle.alert.engine.coordinator.Publishment)
     */
    @Override
    public OpResult addPublishment(Publishment publishment) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#removePublishment(java.lang.String)
     */
    @Override
    public OpResult removePublishment(String pubId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#getScheduleState(java.lang.String)
     */
    @Override
    public ScheduleState getScheduleState(String versionId) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#getScheduleState()
     */
    @Override
    public ScheduleState getScheduleState() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addScheduleState(org.apache.eagle.alert.coordination.model.ScheduleState)
     */
    @Override
    public OpResult addScheduleState(ScheduleState state) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#listAssignments()
     */
    @Override
    public List<PolicyAssignment> listAssignments() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#addAssignment(org.apache.eagle.alert.coordination.model.internal.PolicyAssignment)
     */
    @Override
    public OpResult addAssignment(PolicyAssignment assignment) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#clear()
     */
    @Override
    public OpResult clear() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#export()
     */
    @Override
    public Models export() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.eagle.service.alert.resource.IMetadataDao#importModels(org.apache.eagle.service.alert.resource.Models)
     */
    @Override
    public OpResult importModels(Models models) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<PublishmentType> listPublishmentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OpResult addPublishmentType(PublishmentType publishmentType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OpResult removePublishmentType(String pubType) {
        // TODO Auto-generated method stub
        return null;
    }

}
