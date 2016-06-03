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
package org.apache.eagle.service.metadata.resource;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.impl.MetadataDaoFactory;
import org.apache.eagle.alert.metadata.resource.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.Models;
import org.apache.eagle.alert.metadata.resource.OpResult;

/**
 * @since Apr 11, 2016
 *
 */
@Path("/metadata")
@Produces("application/json")
@Consumes("application/json")
public class MetadataResource {

    private IMetadataDao dao = MetadataDaoFactory.getInstance().getMetadataDao();

    @Path("/clusters")
    @GET
    public List<StreamingCluster> listClusters() {
        return dao.listClusters();
    }
    
    @Path("/clear")
    @POST
    public OpResult clear() {
        return dao.clear();
    }

    @Path("/export")
    @GET
    public Models export() {
        return dao.export();
    }

    @Path("/import")
    @GET
    public OpResult importModels(Models model) {
        return dao.importModels(model);
    }

    @Path("/clusters")
    @POST
    public OpResult addCluster(StreamingCluster cluster) {
        return dao.addCluster(cluster);
    }

    @Path("/clusters/{clusterId}")
    @DELETE
    public OpResult removeCluster(@PathParam("clusterId") String clusterId) {
        return dao.removeCluster(clusterId);
    }

    @Path("/streams")
    @GET
    public List<StreamDefinition> listStreams() {
        return dao.listStreams();
    }

    @Path("/streams")
    @POST
    public OpResult createStream(StreamDefinition stream) {
        return dao.createStream(stream);
    }

    @Path("/streams/{streamId}")
    @DELETE
    public OpResult removeStream(@PathParam("streamId") String streamId) {
        return dao.removeStream(streamId);
    }

    @Path("/datasources")
    @GET
    public List<Kafka2TupleMetadata> listDataSources() {
        return dao.listDataSources();
    }

    @Path("/datasources")
    @POST
    public OpResult addDataSource(Kafka2TupleMetadata dataSource) {
        return dao.addDataSource(dataSource);
    }

    @Path("/datasources/{datasourceId}")
    @DELETE
    public OpResult removeDataSource(@PathParam("datasourceId") String datasourceId) {
        return dao.removeDataSource(datasourceId);
    }

    @Path("/policies")
    @GET
    public List<PolicyDefinition> listPolicies() {
        return dao.listPolicies();
    }

    @Path("/policies")
    @POST
    public OpResult addPolicy(PolicyDefinition policy) {
        return dao.addPolicy(policy);
    }

    @Path("/policies/{policyId}")
    @DELETE
    public OpResult removePolicy(@PathParam("policyId") String policyId) {
        return dao.removePolicy(policyId);
    }

    @Path("/publishments")
    @GET
    public List<Publishment> listPublishment() {
        return dao.listPublishment();
    }

    @Path("/publishments")
    @POST
    public OpResult addPublishment(Publishment publishment) {
        return dao.addPublishment(publishment);
    }

    @Path("/publishments/{pubId}")
    @DELETE
    public OpResult removePublishment(@PathParam("pubId") String pubId) {
        return dao.removePublishment(pubId);
    }

    @Path("/publishmentTypes")
    @GET
    public List<PublishmentType> listPublishmentType() {
        return dao.listPublishmentType();
    }

    @Path("/publishmentTypes")
    @POST
    public OpResult addPublishmentType(PublishmentType publishmentType) {
        return dao.addPublishmentType(publishmentType);
    }

    @Path("/publishmentTypes/{pubType}")
    @DELETE
    public OpResult removePublishmentType(@PathParam("pubType") String pubType) {
        return dao.removePublishmentType(pubType);
    }

    @Path("/schedulestates/{versionId}")
    @GET
    public ScheduleState listScheduleState(@PathParam("versionId") String versionId) {
        return dao.getScheduleState(versionId);
    }

    @Path("/schedulestates")
    @GET
    public ScheduleState latestScheduleState() {
        return dao.getScheduleState();
    }

    @Path("/schedulestates")
    @POST
    public OpResult addScheduleState(ScheduleState state) {
        return dao.addScheduleState(state);
    }

    @Path("/assignments")
    @GET
    public List<PolicyAssignment> listAssignmenets() {
        return dao.listAssignments();
    }

    @Path("/assignments")
    @POST
    public OpResult addAssignmenet(PolicyAssignment pa) {
        return dao.addAssignment(pa);
    }

    @Path("/topologies")
    @GET
    public List<Topology> listTopologies() {
        return dao.listTopologies();
    }

    @Path("/topologies")
    @POST
    public OpResult addTopology(Topology t) {
        return dao.addTopology(t);
    }

    @Path("/topologies/{topologyName}")
    @DELETE
    public OpResult removeTopology(@PathParam("topologyName") String topologyName) {
        return dao.removeTopology(topologyName);
    }

}
