/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.service.memory;

import com.google.common.base.Preconditions;
import com.google.inject.Singleton;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.SiteEntityService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Singleton
public class SiteEntityEntityServiceMemoryImpl implements SiteEntityService {
    private Map<String, SiteEntity> siteId2EntityMap = new HashMap<>();

    @Override
    public Collection<SiteEntity> findAll() {
        return siteId2EntityMap.values();
    }

    @Override
    public SiteEntity getByUUID(String uuid) throws EntityNotFoundException {
        Optional<SiteEntity> entityOptional = siteId2EntityMap.values().stream().filter((site) -> uuid.equals(site.getUuid())).findAny();
        if (entityOptional.isPresent()) {
            return entityOptional.get();
        } else {
            throw new EntityNotFoundException("Site with UUID: " + uuid + " not found");
        }
    }

    @Override
    public SiteEntity create(SiteEntity entity) {
        Preconditions.checkNotNull(entity.getSiteId(), "SiteId is null: " + entity.getSiteId());
        if (siteId2EntityMap.containsKey(entity.getSiteId())) {
            throw new IllegalArgumentException("Duplicated siteId: " + entity.getSiteId());
        }
        entity.ensureDefault();
        siteId2EntityMap.put(entity.getSiteId(), entity);
        return entity;
    }

    @Override
    public SiteEntity getBySiteId(String siteId) throws EntityNotFoundException {
        if (!siteId2EntityMap.containsKey(siteId)) {
            throw new EntityNotFoundException("Site with siteId: " + siteId + " not exists");
        }
        return siteId2EntityMap.get(siteId);
    }

    @Override
    public SiteEntity deleteBySiteId(String siteId) throws EntityNotFoundException {
        return siteId2EntityMap.remove(getBySiteId(siteId).getSiteId());
    }

    @Override
    public SiteEntity deleteByUUID(String uuid) throws EntityNotFoundException {
        return siteId2EntityMap.remove(getByUUID(uuid).getSiteId());
    }

    @Override
    public SiteEntity update(SiteEntity siteEntity) throws EntityNotFoundException {
        if (siteEntity.getSiteId() == null && siteEntity.getUuid() == null) {
            throw new IllegalArgumentException("siteId and UUID are both null, don't know how to update");
        }
        SiteEntity oldEntity = (siteEntity.getSiteId() != null) ? getBySiteId(siteEntity.getSiteId()) : getByUUID(siteEntity.getUuid());
        siteEntity.setUuid(oldEntity.getUuid());
        siteEntity.setCreatedTime(oldEntity.getCreatedTime());
        siteEntity.ensureDefault();
        siteId2EntityMap.put(siteEntity.getSiteId(), siteEntity);
        return siteEntity;
    }
}