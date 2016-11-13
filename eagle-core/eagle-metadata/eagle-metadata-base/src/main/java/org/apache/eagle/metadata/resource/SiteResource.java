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
package org.apache.eagle.metadata.resource;

import org.apache.eagle.metadata.exceptions.SiteDeleteException;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

@Path("/sites")
@Singleton
public class SiteResource {
    private final SiteEntityService siteEntityService;
    private final ApplicationEntityService entityService;

    @Inject
    public SiteResource(SiteEntityService siteEntityService,
                        ApplicationEntityService applicationEntityService) {
        this.siteEntityService = siteEntityService;
        this.entityService = applicationEntityService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<SiteEntity>> getAllSites() {
        return RESTResponse.async(siteEntityService::findAll).get();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> createSite(SiteEntity siteEntity) {
        return RESTResponse.<SiteEntity>async((builder) -> {
            SiteEntity entity = siteEntityService.create(siteEntity);
            builder.message("Successfully created site (siteId:" + entity.getSiteId() + ", uuid: " + entity.getUuid() + ")");
            builder.data(entity);
        }).get();
    }

    @GET
    @Path("/{siteId}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> getSiteBySiteId(@PathParam("siteId") String siteId) {
        return RESTResponse.async(() -> siteEntityService.getBySiteId(siteId)).get();
    }

    @DELETE
    @Path("/{siteId}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> deleteSiteBySiteId(@PathParam("siteId") String siteId) {
        return RESTResponse.async(() -> {
            int appCount = entityService.findBySiteId(siteId).size();
            if (appCount > 0) {
                throw new SiteDeleteException("This site has enabled applications, remove them first");
            }
            return siteEntityService.deleteBySiteId(siteId);
        }).get();
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> deleteSiteByUUID(UUIDRequest uuidRequest) {
        return RESTResponse.async(() -> {
            int appCount = entityService.findBySiteId(siteEntityService.getByUUID(uuidRequest.getUuid()).getSiteId()).size();
            if (appCount > 0) {
                throw new SiteDeleteException("This site has enabled applications, remove them first");
            }
            return siteEntityService.deleteByUUID(uuidRequest.getUuid());
        }).get();
    }

    @PUT
    @Path("/{siteId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> updateSite(@PathParam("siteId") String siteId, SiteEntity siteEntity) {
        return RESTResponse.async(() -> {
            siteEntity.setSiteId(siteId);
            return siteEntityService.update(siteEntity);
        }).get();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> updateSite(SiteEntity siteEntity) {
        return RESTResponse.async(() -> {
            return siteEntityService.update(siteEntity);
        }).get();
    }
}