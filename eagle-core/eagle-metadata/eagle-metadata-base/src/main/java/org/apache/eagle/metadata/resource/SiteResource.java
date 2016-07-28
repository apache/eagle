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

import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.SiteEntityService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

import static org.apache.eagle.metadata.resource.RESTResponse.async;

@Path("/sites")
@Singleton
public class SiteResource {
    private final SiteEntityService siteEntityService;

    @Inject
    public SiteResource(SiteEntityService siteEntityService){
        this.siteEntityService = siteEntityService;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<SiteEntity> getAllSites(){
        return siteEntityService.findAll();
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<SiteEntity> createSite(SiteEntity siteEntity){
        return RESTResponse.<SiteEntity>async((builder)-> {
            SiteEntity entity = siteEntityService.create(siteEntity);
            builder.message("Successfully created site (siteId:"+entity.getSiteId()+", uuid: "+entity.getUuid()+")");
            builder.data(entity);
        }).get();
    }

    @GET
    @Path("/{siteIdOrUUID}")
    @Produces(MediaType.APPLICATION_JSON)
    public SiteEntity getSiteByNameOrUUID(@PathParam("siteIdOrUUID") String siteIdOrUUID){
        return siteEntityService.getBySiteIdOrUUID(siteIdOrUUID);
    }
}