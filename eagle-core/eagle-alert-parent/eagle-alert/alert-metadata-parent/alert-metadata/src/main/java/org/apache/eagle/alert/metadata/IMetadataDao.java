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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.metadata.resource.Models;
import org.apache.eagle.alert.metadata.resource.OpResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PathParam;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    List<AlertPublishEvent> listAlertPublishEvent(int size);

    AlertPublishEvent getAlertPublishEvent(String alertId);

    List<AlertPublishEvent> getAlertPublishEventsByPolicyId(String policyId, int size);

    OpResult addAlertPublishEvent(AlertPublishEvent event);

    ScheduleState getScheduleState(String versionId);

    ScheduleState getScheduleState();

    List<ScheduleState> listScheduleStates();

    OpResult addScheduleState(ScheduleState state);

    OpResult clearScheduleState(int maxCapacity);

    List<PolicyAssignment> listAssignments();

    OpResult addAssignment(PolicyAssignment assignment);

    // APIs for test friendly
    OpResult clear();

    Models export();

    OpResult importModels(Models models);

    // -----------------------------------------------------------
    //  Extended Metadata DAO Methods with default implementation
    // -----------------------------------------------------------

    Logger LOG = LoggerFactory.getLogger(IMetadataDao.class);

    default PolicyDefinition getPolicyById(String policyId) {
        Preconditions.checkNotNull(policyId,"policyId");
        return listPolicies().stream().filter(pc -> pc.getName().equals(policyId)).findAny().orElseGet(() -> {
            LOG.error("Policy (policyId " + policyId + ") not found");
            throw new IllegalArgumentException("Policy (policyId " + policyId + ") not found");
        });
    }

    default List<Publishment> getPublishmentsByPolicyId(String policyId) {
        return listPublishment().stream().filter(ps ->
                ps.getPolicyIds() != null && ps.getPolicyIds().contains(policyId)
        ).collect(Collectors.toList());
    }

    default OpResult addPublishmentsToPolicy(String policyId, List<String> publishmentIds) {
        OpResult result = new OpResult();
        if (publishmentIds == null || publishmentIds.size() == 0) {
            result.code = OpResult.FAILURE;
            result.message = "Failed to add policy, there is no publisher in it";
            return result;
        }
        try {
            Map<String,Publishment> publishmentMap = new HashMap<>();
            listPublishment().forEach((pub) -> publishmentMap.put(pub.getName(),pub));
            for (String publishmentId : publishmentIds) {
                if (publishmentMap.containsKey(publishmentId)) {
                    Publishment publishment = publishmentMap.get(publishmentId);
                    if (publishment.getPolicyIds() == null) {
                        publishment.setPolicyIds(new ArrayList<>());
                    }
                    if (publishment.getPolicyIds().contains(policyId)) {
                        LOG.warn("Policy {} was already bound with publisher {}",policyId, publishmentId);
                    } else {
                        publishment.getPolicyIds().add(policyId);
                    }
                    OpResult opResult = addPublishment(publishment);
                    if (opResult.code == OpResult.FAILURE) {
                        LOG.error("Failed to add publisher {} to policy {}: {}", publishmentId, policyId, opResult.message);
                        return opResult;
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(opResult.message);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Publishment (name: " + publishmentId + ") not found");
                }
            }

            //for other publishments, remove policyId from them, work around, we should refactor
            for (String publishmentId : publishmentMap.keySet()) {
                if (publishmentIds.contains(publishmentId)) {
                    continue;
                }
                Publishment publishment = publishmentMap.get(publishmentId);
                if (publishment.getPolicyIds() != null && publishment.getPolicyIds().contains(policyId)) {
                    publishment.getPolicyIds().remove(policyId);
                    OpResult opResult = addPublishment(publishment);
                    if (opResult.code == OpResult.FAILURE) {
                        LOG.error("Failed to delete policy {}, from publisher {}, {} ", policyId, publishmentId, opResult.message);
                        return opResult;
                    }
                }
            }
            result.code = OpResult.SUCCESS;
            result.message = "Successfully add " + publishmentIds.size() + " publishments: [" + StringUtils.join(publishmentIds,",") + "] to policy: " + policyId;
            LOG.info(result.message);
        } catch (Exception ex) {
            result.code = OpResult.FAILURE;
            result.message = "Failed to add publishments: [" + StringUtils.join(publishmentIds,",") + "] to policy: " + policyId + ", cause: " + ex.getMessage();
            LOG.error(result.message,ex);
        }
        return result;
    }
}
