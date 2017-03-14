/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.service.memory;

import com.google.common.base.Preconditions;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.DashboardEntityService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DashboardEntityServiceMemoryImpl implements DashboardEntityService {
    private final Map<String, DashboardEntity> dashboardEntityMap = new HashMap<>();

    @Override
    public synchronized Collection<DashboardEntity> findAll() {
        return dashboardEntityMap.values();
    }

    @Override
    public synchronized DashboardEntity getByUUID(String uuid) throws EntityNotFoundException {
        Preconditions.checkNotNull(uuid, "uuid should not be null");
        if (!dashboardEntityMap.containsKey(uuid)) {
            throw new EntityNotFoundException("uuid " + uuid + "not exist");
        }
        return dashboardEntityMap.get(uuid);
    }

    @Override
    public synchronized DashboardEntity create(DashboardEntity entity) {
        Preconditions.checkNotNull(entity, "DashboardEntity is null");
        Preconditions.checkArgument(entity.getUuid() == null, "Dashboard Entity uuid should be null");
        entity.ensureDefault();
        try {
            Preconditions.checkArgument(getByUUIDOrName(entity.getUuid(), entity.getName()) == null, "Duplicated dashboard name");
        } catch (EntityNotFoundException e) {
            // ignore
        }
        dashboardEntityMap.put(entity.getUuid(), entity);
        return entity;
    }

    @Override
    public synchronized DashboardEntity update(DashboardEntity dashboardEntity) throws EntityNotFoundException {
        Preconditions.checkNotNull(dashboardEntity, "Dashboard Entity is null");
        Preconditions.checkNotNull(dashboardEntity.getUuid(), "Dashboard Entity uuid should not be null");
        if (!dashboardEntityMap.containsKey(dashboardEntity.getUuid())) {
            throw new EntityNotFoundException("Dashboard (uuid: " + dashboardEntity.getUuid() + ") not exist");
        }
        dashboardEntity.ensureDefault();
        dashboardEntityMap.put(dashboardEntity.getUuid(), dashboardEntity);
        return dashboardEntity;
    }

    @Override
    public synchronized DashboardEntity getByUUIDOrName(String uuid, String name) throws EntityNotFoundException {
        if (uuid != null) {
            return getByUUID(uuid);
        } else if (name != null) {
            return dashboardEntityMap.values().stream()
                .filter((dashboardEntity -> dashboardEntity.getName().equals(name))).findAny()
                .orElseThrow(() -> new EntityNotFoundException("Dashboard named: " + name + " not found"));
        }
        throw new IllegalArgumentException("Both uuid and name are null");
    }

    @Override
    public synchronized DashboardEntity deleteByUUID(String uuid) throws EntityNotFoundException {
        Preconditions.checkNotNull(uuid, "UUID should not be null");
        if (dashboardEntityMap.containsKey(uuid)) {
            return dashboardEntityMap.remove(uuid);
        }
        throw new EntityNotFoundException("Dashboard (uuid: " + uuid + ") not found");
    }
}