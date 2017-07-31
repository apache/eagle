/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.metadata.resource;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.interpreter.PolicyValidationResult;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.metadata.model.PolicyEntity;
import org.apache.eagle.metadata.service.PolicyEntityService;
import org.apache.eagle.metadata.utils.PolicyIdConversions;
import org.apache.eagle.metadata.utils.StreamIdConversions;
import org.apache.eagle.service.metadata.resource.MetadataResource;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Path("/policyProto")
public class PolicyResource {
    private final PolicyEntityService policyEntityService;
    private final MetadataResource metadataResource;

    @Inject
    public PolicyResource(PolicyEntityService policyEntityService, MetadataResource metadataResource) {
        this.policyEntityService = policyEntityService;
        this.metadataResource = metadataResource;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<PolicyEntity>> getAllPolicyProto() {
        return RESTResponse.async(policyEntityService::getAllPolicyProto).get();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<PolicyEntity> createOrUpdatePolicyProto(PolicyEntity policyProto) {
        return RESTResponse.async(() -> policyEntityService.createOrUpdatePolicyProto(policyProto)).get();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<PolicyEntity> saveAsPolicyProto(PolicyEntity policyEntity,
                                                        @QueryParam("needPolicyProtoCreated") boolean needPolicyProtoCreated) {
        return RESTResponse.async(() -> {
            Preconditions.checkNotNull(policyEntity, "entity should not be null");
            Preconditions.checkNotNull(policyEntity.getDefinition(), "policy definition should not be null");
            Preconditions.checkNotNull(policyEntity.getAlertPublishmentIds(), "alert publisher list should not be null");

            PolicyDefinition policyDefinition = policyEntity.getDefinition();
            checkOutputStream(policyDefinition.getInputStreams(), policyDefinition.getOutputStreams());
            OpResult result = metadataResource.addPolicy(policyDefinition);
            if (result.code != 200) {
                throw new IllegalArgumentException(result.message);
            }
            result = metadataResource.addPublishmentsToPolicy(policyDefinition.getName(), policyEntity.getAlertPublishmentIds());
            if (result.code != 200) {
                throw new IllegalArgumentException(result.message);
            }
            if (needPolicyProtoCreated) {
                importPolicyProto(policyEntity);
            }
            return policyEntity;
        }).get();
    }

    @POST
    @Path("/create/{policyId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<PolicyEntity> saveAsPolicyProto(@PathParam("policyId") String policyId) {
        return RESTResponse.async(() -> {
            Preconditions.checkNotNull(policyId, "policyId should not be null");
            PolicyDefinition policyDefinition = metadataResource.getPolicyById(policyId);

            if (policyDefinition == null) {
                throw new IllegalArgumentException("policy does not exist: " + policyId);
            }

            PolicyEntity policyEntity = new PolicyEntity();
            policyEntity.setDefinition(policyDefinition);

            List<Publishment> alertPublishments = metadataResource.getPolicyPublishments(policyId);
            if (alertPublishments != null && !alertPublishments.isEmpty()) {
                List<String> alertPublisherIds = new ArrayList<>();
                for (Publishment publishment : alertPublishments) {
                    alertPublisherIds.add(publishment.getName());
                }
                policyEntity.setAlertPublishmentIds(alertPublisherIds);
            }
            return importPolicyProto(policyEntity);
        }).get();
    }

    @POST
    @Path("/export/{site}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Boolean> loadPoliciesByProto(List<PolicyEntity> policyProtoList, @PathParam("site") String site) {
        return RESTResponse.async(() -> exportPolicyProto(policyProtoList, site)).get();
    }

    @POST
    @Path("/exportByName/{site}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Boolean> loadPoliciesByName(List<String> policyProtoList, @PathParam("site") String site) {
        return RESTResponse.async(() -> {
            if (policyProtoList == null || policyProtoList.isEmpty()) {
                throw new IllegalArgumentException("policyProtoList is null or empty");
            }
            List<PolicyEntity> policyEntities = new ArrayList<PolicyEntity>();
            for (String name : policyProtoList) {
                PolicyEntity entity = policyEntityService.getByUUIDorName(null, name);
                if (entity != null) {
                    policyEntities.add(entity);
                }
            }
            return exportPolicyProto(policyEntities, site);
        }).get();
    }

    @DELETE
    @Path("/{uuid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Boolean> deletePolicyProto(@PathParam("uuid") String uuid) {
        return RESTResponse.async(() -> policyEntityService.deletePolicyProtoByUUID(uuid)).get();
    }

    private PolicyEntity importPolicyProto(PolicyEntity policyEntity) {
        PolicyDefinition policyDefinition = policyEntity.getDefinition();
        checkOutputStream(policyDefinition.getInputStreams(), policyDefinition.getOutputStreams());
        List<String> inputStreamType = new ArrayList<>();
        String newDefinition = policyDefinition.getDefinition().getValue();
        for (String inputStream : policyDefinition.getInputStreams()) {
            String streamDef = StreamIdConversions.parseStreamTypeId(policyDefinition.getSiteId(), inputStream);
            inputStreamType.add(streamDef);
            newDefinition = newDefinition.replaceAll(inputStream, streamDef);
        }
        policyDefinition.setInputStreams(inputStreamType);
        policyDefinition.getDefinition().setValue(newDefinition);
        policyDefinition.setName(PolicyIdConversions.parsePolicyId(policyDefinition.getSiteId(), policyDefinition.getName()));
        policyDefinition.setSiteId(null);
        policyDefinition.getPartitionSpec().clear();
        policyEntity.setDefinition(policyDefinition);
        return policyEntityService.createOrUpdatePolicyProto(policyEntity);
    }

    private Boolean exportPolicyProto(List<PolicyEntity> policyProtoList, String site) {
        Preconditions.checkNotNull(site, "site should not be null");
        if (policyProtoList == null || policyProtoList.isEmpty()) {
            throw new IllegalArgumentException("policy prototype list is empty or null");
        }
        for (PolicyEntity policyProto : policyProtoList) {
            PolicyDefinition policyDefinition = policyProto.getDefinition();
            List<String> inputStreams = new ArrayList<>();
            String newDefinition = policyDefinition.getDefinition().getValue();
            for (String inputStreamType : policyDefinition.getInputStreams()) {
                String streamId = StreamIdConversions.formatSiteStreamId(site, inputStreamType);
                inputStreams.add(streamId);
                newDefinition = newDefinition.replaceAll(inputStreamType, streamId);
            }
            policyDefinition.setInputStreams(inputStreams);
            policyDefinition.getDefinition().setValue(newDefinition);
            policyDefinition.setSiteId(site);
            policyDefinition.setName(PolicyIdConversions.generateUniquePolicyId(site, policyProto.getDefinition().getName()));

            PolicyValidationResult validationResult = metadataResource.validatePolicy(policyDefinition);
            if (!validationResult.isSuccess() || validationResult.getException() != null) {
                throw new IllegalArgumentException(validationResult.getException());
            }

            policyDefinition.getPartitionSpec().clear();
            for (StreamPartition sd : validationResult.getPolicyExecutionPlan().getStreamPartitions()) {
                if (inputStreams.contains(sd.getStreamId())) {
                    policyDefinition.getPartitionSpec().add(sd);
                }
            }

            OpResult result = metadataResource.addPolicy(policyDefinition);
            if (result.code != 200) {
                throw new IllegalArgumentException("fail to create policy: " + result.message);
            }
            if (policyProto.getAlertPublishmentIds() != null && !policyProto.getAlertPublishmentIds().isEmpty()) {
                result = metadataResource.addPublishmentsToPolicy(policyDefinition.getName(), policyProto.getAlertPublishmentIds());
                if (result.code != 200) {
                    throw new IllegalArgumentException("fail to create policy publisherments: " + result.message);
                }
            }
        }
        return true;
    }

    private void checkOutputStream(List<String> inputStreams, List<String> outputStreams) {
        for (String inputStream : inputStreams) {
            for (String outputStream : outputStreams) {
                if (outputStream.contains(inputStream)) {
                    throw new IllegalArgumentException("OutputStream name should not contains string: " + inputStream
                            + ". Please rename your OutputStream name");
                }
            }
        }
    }

}
