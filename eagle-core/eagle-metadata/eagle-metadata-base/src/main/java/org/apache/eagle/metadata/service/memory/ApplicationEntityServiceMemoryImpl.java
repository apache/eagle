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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationDescService;
import org.apache.eagle.metadata.service.ApplicationEntityService;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class ApplicationEntityServiceMemoryImpl implements ApplicationEntityService {

    private final ApplicationDescService applicationDescService;
    private final Map<String,ApplicationEntity> applicationEntityMap;

    @Inject
    public ApplicationEntityServiceMemoryImpl(ApplicationDescService applicationDescService){
        this.applicationDescService = applicationDescService;
        this.applicationEntityMap = new HashMap<>();
    }

    @Override
    public Collection<ApplicationEntity> findAll() {
        return applicationEntityMap.values();
    }

    @Override
    public ApplicationEntity getByUUID(String uuid) {
        if(applicationEntityMap.containsKey(uuid)) {
            return applicationEntityMap.get(uuid);
        }else{
            throw new IllegalArgumentException("Application with UUID: "+uuid);
        }
    }

    @Override
    public ApplicationEntity create(ApplicationEntity entity) {
        entity.ensureDefault();
        if(getBySiteIdAndAppType(entity.getSite().getSiteId(),entity.getDescriptor().getType()) != null)
            throw new IllegalArgumentException("Duplicated appId: "+entity.getAppId());
        applicationEntityMap.put(entity.getUuid(),entity);
        return entity;
    }

    @Override
    public Collection<ApplicationEntity> findBySiteId(String siteId) {
        return applicationEntityMap.values().stream().filter((app) -> siteId.equals(app.getSite().getSiteId())).collect(Collectors.toList());
    }

    @Override
    public ApplicationEntity getBySiteIdAndAppType(String siteId, String appType) {
        Optional<ApplicationEntity>  optional =
                applicationEntityMap.values().stream()
                        .filter((app) -> siteId.equals(app.getSite().getSiteId()) && appType.equals(app.getDescriptor().getType())).findAny();
        if(optional.isPresent()){
            return optional.get();
        } else {
            return null;
        }
    }

    @Override
    public ApplicationEntity getByUUIDOrAppId(String uuid, String appId) {
        if(uuid == null && appId == null)
            throw new IllegalArgumentException("uuid and appId are both null");
        else if(uuid !=null){
            return getByUUID(uuid);
        }else {
            Optional<ApplicationEntity> applicationEntity = applicationEntityMap.values().stream().filter((app) -> appId.equals(app.getAppId())).findAny();
            if(applicationEntity.isPresent()){
                return applicationEntity.get();
            }else{
                throw new IllegalArgumentException("Application with appId: "+appId+" not found");
            }
        }
    }

    @Override
    public ApplicationEntity delete(ApplicationEntity applicationEntity) {
        ApplicationEntity entity = getByUUIDOrAppId(applicationEntity.getUuid(),applicationEntity.getAppId());
        return applicationEntityMap.remove(entity.getUuid());
    }
}