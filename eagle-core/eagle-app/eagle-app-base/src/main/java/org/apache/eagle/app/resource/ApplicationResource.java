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
package org.apache.eagle.app.resource;


import com.google.inject.Inject;
import org.apache.eagle.app.service.ApplicationManagementService;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.resource.RESTResponse;
import org.apache.eagle.metadata.service.ApplicationEntityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

@Path("/apps")
public class ApplicationResource {
    private final ApplicationProviderService providerService;
    private final ApplicationManagementService applicationManagementService;
    private final ApplicationEntityService entityService;

    @Inject
    public ApplicationResource(
            ApplicationProviderService providerService,
            ApplicationManagementService applicationManagementService,
            ApplicationEntityService entityService){
        this.providerService = providerService;
        this.applicationManagementService = applicationManagementService;
        this.entityService = entityService;
    }

    @GET
    @Path("/providers")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<ApplicationDesc> getApplicationDescs(){
        return providerService.getApplicationDescs();
    }

    @GET
    @Path("/providers/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApplicationDesc getApplicationDescs(@PathParam("type") String type){
        return providerService.getApplicationDescByType(type);
    }

    @PUT
    @Path("/providers/reload")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationDesc>> reloadApplicationDescs(){
        return RESTResponse.<Collection<ApplicationDesc>>async((builder)-> {
            providerService.reload();
            builder.message("Successfully reload application providers");
            builder.data(providerService.getApplicationDescs());
        }).get();
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<ApplicationEntity> getApplicationEntities(@QueryParam("siteId") String siteId){
        if(siteId == null) {
            return entityService.findAll();
        } else {
            return entityService.findBySiteId(siteId);
        }
    }

    @GET
    @Path("/{appUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApplicationEntity getApplicationEntityByUUID(@PathParam("appUuid") String appUuid){
        return entityService.getByUUID(appUuid);
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>
     */
    @POST
    @Path("/install")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity> installApplication(ApplicationOperations.InstallOperation operation){
        return RESTResponse.<ApplicationEntity>async((builder)-> {
            ApplicationEntity entity = applicationManagementService.install(operation);
            builder.message("Successfully installed application "+operation.getAppType()+" onto site "+operation.getSiteId());
            builder.data(entity);
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>
     *
     * @param operation
     */
    @DELETE
    @Path("/uninstall")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> uninstallApplication(ApplicationOperations.UninstallOperation operation){
        return RESTResponse.<Void>async((builder)-> {
            ApplicationEntity entity = applicationManagementService.uninstall(operation);
            builder.success(true).message("Successfully uninstalled application "+entity.getUuid());
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     *operation
     * @param operation
     */
    @POST
    @Path("/start")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> startApplication(ApplicationOperations.StartOperation operation){
        return RESTResponse.<Void>async((builder)-> {
            ApplicationEntity entity = applicationManagementService.start(operation);
            builder.success(true).message("Successfully started application "+entity.getUuid());
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>
     * @param operation
     */
    @POST
    @Path("/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> stopApplication(ApplicationOperations.StopOperation operation){
        return RESTResponse.<Void>async((builder)-> {
            ApplicationEntity entity = applicationManagementService.stop(operation);
            builder.success(true).message("Successfully stopped application "+entity.getUuid());
        }).get();
    }
}