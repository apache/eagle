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
    @Path("saveAsProto")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<PolicyEntity> saveAsPolicyProto(PolicyEntity policyEntity) {
        Preconditions.checkNotNull(policyEntity, "policyDefinition should not be null");
        PolicyDefinition policyDefinition = policyEntity.getPolicyProto();
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

        policyEntity.setPolicyProto(policyDefinition);
        return createOrUpdatePolicyProto(policyEntity);
    }

    @POST
    @Path("loadToSite/{site}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<List<PolicyDefinition>> loadPolicyDefinition(List<PolicyEntity> policyProtoList, @PathParam("site") String site) {
        return RESTResponse.async(() -> importPolicyDefinition(policyProtoList, site)).get();

    }

    @DELETE
    @Path("/{uuid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Boolean> deletePolicyProto(@PathParam("uuid") String uuid) {
        return RESTResponse.async(() -> policyEntityService.deletePolicyProtoByUUID(uuid)).get();
    }


    private List<PolicyDefinition> importPolicyDefinition(List<PolicyEntity> policyProtoList, String site) {
        Preconditions.checkNotNull(site, "site should not be null");
        if (policyProtoList == null || policyProtoList.isEmpty()) {
            throw new IllegalArgumentException("policy prototype list is empty or null");
        }
        List<PolicyDefinition> policies = new ArrayList<>();
        for (PolicyEntity policyProto : policyProtoList) {
            PolicyDefinition policyDefinition = policyProto.getPolicyProto();
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
            policyDefinition.setName(PolicyIdConversions.generateUniquePolicyId(site, policyProto.getPolicyProto().getName()));
            PolicyValidationResult validationResult = metadataResource.validatePolicy(policyDefinition);
            if (!validationResult.isSuccess() || validationResult.getException() != null) {
                throw new IllegalArgumentException(validationResult.getException());
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
            policies.add(policyDefinition);
        }
        return policies;
    }
}
