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

import com.google.inject.Singleton;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.SiteEntityService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class SiteEntityEntityServiceMemoryImpl implements SiteEntityService {
    private Map<String,SiteEntity> nameSiteMap = new HashMap<>();

    @Override
    public Collection<SiteEntity> findAll() {
        return nameSiteMap.values();
    }

    @Override
    public SiteEntity getByUUID(String uuid) {
        return nameSiteMap.values().stream().filter((site) -> uuid.equals(site.getUuid())).findAny().get();
    }

    @Override
    public SiteEntity create(SiteEntity entity) {
        if(getBySiteId(entity.getSiteId()) != null){
            throw new IllegalArgumentException("Duplicated site: "+entity);
        }
        entity.ensureDefault();
        nameSiteMap.put(entity.getSiteId(),entity);
        return entity;
    }

    @Override
    public SiteEntity getBySiteId(String siteName) {
        return nameSiteMap.get(siteName);
    }

    @Override
    public SiteEntity getBySiteIdOrUUID(String siteNameOrUUID) {
        if(nameSiteMap.containsKey(siteNameOrUUID)){
            return nameSiteMap.get(siteNameOrUUID);
        } else {
            return getByUUID(siteNameOrUUID);
        }
    }
}