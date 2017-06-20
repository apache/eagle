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

package org.apache.eagle.metadata.service;

import com.google.common.base.Preconditions;
import org.apache.eagle.metadata.model.PolicyEntity;
import org.apache.eagle.metadata.persistence.PersistenceService;

import java.util.Collection;


public interface PolicyEntityService {

    Collection<PolicyEntity> getAllPolicyProto();

    PolicyEntity getPolicyProtoByUUID(String uuid);

    boolean deletePolicyProtoByUUID(String uuid);

    default PolicyEntity createOrUpdatePolicyProto(PolicyEntity policyProto) {
        Preconditions.checkNotNull(policyProto, "DashboardEntity should not be null");
        if (policyProto.getUuid() == null) {
            return create(policyProto);
        } else {
            return update(policyProto);
        }
    }

    PolicyEntity create(PolicyEntity policyEntity);

    PolicyEntity update(PolicyEntity policyEntity);

}
