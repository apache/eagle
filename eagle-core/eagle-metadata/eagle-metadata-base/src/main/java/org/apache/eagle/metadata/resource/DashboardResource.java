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
package org.apache.eagle.metadata.resource;


import com.google.inject.Inject;
import io.dropwizard.auth.Auth;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.common.security.RolesAllowed;
import org.apache.eagle.common.security.User;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.DashboardEntityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

@Path("/dashboards")
public class DashboardResource {
    @Inject
    private DashboardEntityService dashboardEntityService;

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<DashboardEntity>> listAllDashboards() {
        return RESTResponse.async(() -> dashboardEntityService.findAll()).get();
    }

    @GET
    @Path("/{uuidOrName}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<DashboardEntity> getDashboardByUUIDOrName(@PathParam("uuidOrName") String uuidOrName) {
        return RESTResponse.async(() -> dashboardEntityService.getByUUIDOrName(uuidOrName, uuidOrName)).get();
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @RolesAllowed({User.Role.USER, User.Role.ADMINISTRATOR})
    public RESTResponse<DashboardEntity> createOrUpdateDashboard(DashboardEntity dashboardEntity, @Auth User user) {
        return RESTResponse.async(() -> dashboardEntityService.createOrUpdate(dashboardEntity, user)).get();
    }

    @DELETE
    @Path("/{uuid}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @RolesAllowed({User.Role.USER, User.Role.ADMINISTRATOR})
    public RESTResponse<DashboardEntity> deleteDashboard(String uuid, @Auth User user) {
        return RESTResponse.async(() -> dashboardEntityService.deleteByUUID(uuid, user)).get();
    }
}