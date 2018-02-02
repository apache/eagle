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

package org.apache.eagle.metadata.service.memory;

import com.google.common.base.Preconditions;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.PolicyEntity;
import org.apache.eagle.metadata.service.PolicyEntityService;

import java.util.Collection;
import java.util.HashMap;

public class PolicyEntityServiceMemoryImpl implements PolicyEntityService {
    private HashMap<String, PolicyEntity> policyProtoMap = new HashMap<>();

    @Override
    public Collection<PolicyEntity> getAllPolicyProto() {
        return policyProtoMap.values();
    }

    @Override
    public PolicyEntity getByUUIDorName(String uuid, String name) {
        Preconditions.checkArgument(uuid != null || name != null, "Both uuid and name are null");
        if (uuid != null) {
            return policyProtoMap.get(uuid);
        } else {
            return policyProtoMap.values().stream().filter(o -> o.getName().equals(name)).findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Policy proto named " + name + " is not found"));
        }
    }

    @Override
    public boolean deletePolicyProtoByUUID(String uuid) {
        policyProtoMap.remove(uuid);
        return true;
    }


    @Override
    public PolicyEntity create(PolicyEntity entity) {
        Preconditions.checkNotNull(entity, "entity is null: " + entity);
        entity.ensureDefault();
        policyProtoMap.put(entity.getUuid(), entity);
        return entity;
    }

    @Override
    public PolicyEntity update(PolicyEntity policyEntity) {
        Preconditions.checkNotNull(policyEntity, "entity is null: " + policyEntity);
        Preconditions.checkNotNull(policyEntity.getUuid(), "uuid is null: " + policyEntity.getUuid());
        policyProtoMap.put(policyEntity.getUuid(), policyEntity);
        return policyEntity;
    }
}
