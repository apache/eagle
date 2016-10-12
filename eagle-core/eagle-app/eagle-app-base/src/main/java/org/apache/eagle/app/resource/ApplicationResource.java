/*
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


import org.apache.eagle.app.service.ApplicationManagementService;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.resource.RESTResponse;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import com.google.inject.Inject;

import java.util.Collection;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/apps")
public class ApplicationResource {
    private final ApplicationProviderService providerService;
    private final ApplicationManagementService applicationManagementService;
    private final ApplicationEntityService entityService;

    @Inject
    public ApplicationResource(
        ApplicationProviderService providerService,
        ApplicationManagementService applicationManagementService,
        ApplicationEntityService entityService) {
        this.providerService = providerService;
        this.applicationManagementService = applicationManagementService;
        this.entityService = entityService;
    }

    @GET
    @Path("/providers")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationDesc>> getApplicationDescs() {
        return RESTResponse.async(providerService::getApplicationDescs).get();
    }

    @GET
    @Path("/providers/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationDesc> getApplicationDescByType(@PathParam("type") String type) {
        return RESTResponse.async(() -> providerService.getApplicationDescByType(type)).get();
    }

    @PUT
    @Path("/providers/reload")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationDesc>> reloadApplicationDescs() {
        return RESTResponse.<Collection<ApplicationDesc>>async((response) -> {
            providerService.reload();
            response.message("Successfully reload application providers");
            response.data(providerService.getApplicationDescs());
        }).get();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationEntity>> getApplicationEntities(@QueryParam("siteId") String siteId) {
        return RESTResponse.async(() -> {
            if (siteId == null) {
                return entityService.findAll();
            } else {
                return entityService.findBySiteId(siteId);
            }
        }).get();
    }

    @GET
    @Path("/{appUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity> getApplicationEntityByUUID(@PathParam("appUuid") String appUuid) {
        return RESTResponse.async(() -> entityService.getByUUID(appUuid)).get();
    }

    @POST
    @Path("/{appUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity> updateApplicationEntity(@PathParam("appUuid") String appUuid, ApplicationOperations.UpdateOperation updateOperation) {
        return RESTResponse.async(() -> {
            ApplicationEntity applicationEntity = new ApplicationEntity();
            applicationEntity.setUuid(appUuid);
            applicationEntity.setJarPath(updateOperation.getJarPath());
            applicationEntity.setMode(updateOperation.getMode());
            applicationEntity.setConfiguration(updateOperation.getConfiguration());
            return entityService.update(applicationEntity);
        }).get();
    }

    @POST
    @Path("/status")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity.Status> checkApplicationStatusByUUID(ApplicationOperations.CheckStatusOperation operation) {
        return RESTResponse.<ApplicationEntity.Status>async((response) -> {
            ApplicationEntity.Status status = (entityService.getByUUIDOrAppId(null, operation.getAppId())).getStatus();
            response.success(true).message("Successfully fetched application status");
            response.data(status);
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>.
     */
    @POST
    @Path("/install")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity> installApplication(ApplicationOperations.InstallOperation operation) {
        return RESTResponse.<ApplicationEntity>async((response) -> {
            ApplicationEntity entity = applicationManagementService.install(operation);
            response.message("Successfully installed application " + operation.getAppType() + " onto site " + operation.getSiteId());
            response.data(entity);
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>.
     *
     * @param operation
     */
    @DELETE
    @Path("/uninstall")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> uninstallApplication(ApplicationOperations.UninstallOperation operation) {
        return RESTResponse.<Void>async((response) -> {
            ApplicationEntity entity = applicationManagementService.uninstall(operation);
            response.success(true).message("Successfully uninstalled application " + entity.getUuid());
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>
     * operation.
     * @param operation
     */
    @POST
    @Path("/start")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> startApplication(ApplicationOperations.StartOperation operation) {
        return RESTResponse.<Void>async((response) -> {
            ApplicationEntity entity = applicationManagementService.start(operation);
            response.success(true).message("Starting application " + entity.getUuid());
        }).get();
    }

    /**
     * <b>Request:</b>
     * <pre>
     * {
     *      uuid: APPLICATION_UUID
     * }
     * </pre>.
     *
     * @param operation
     */
    @POST
    @Path("/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> stopApplication(ApplicationOperations.StopOperation operation) {
        return RESTResponse.<Void>async((response) -> {
            ApplicationEntity entity = applicationManagementService.stop(operation);
            response.success(true).message("Stopping application " + entity.getUuid());
        }).get();
    }

}