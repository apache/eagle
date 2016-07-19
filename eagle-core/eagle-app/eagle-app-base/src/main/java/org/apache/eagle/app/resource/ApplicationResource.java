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
import org.apache.eagle.app.manager.ApplicationManagementService;
import org.apache.eagle.app.manager.ApplicationOperations;
import org.apache.eagle.app.manager.ApplicationProviderService;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
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

    @POST
    @Path("/providers/reload")
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<ApplicationDesc> reloadApplicationDescs(){
        providerService.reload();
        return providerService.getApplicationDescs();
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
    public ApplicationEntity installApplication(ApplicationOperations.InstallOperation operation){
        return applicationManagementService.install(operation);
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
    @POST
    @Path("/uninstall")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ApplicationEntity uninstallApplication(ApplicationOperations.UninstallOperation operation){
        return applicationManagementService.uninstall(operation);
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
    public ApplicationEntity startApplication(ApplicationOperations.StartOperation operation){
        return applicationManagementService.start(operation);
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
    public ApplicationEntity stopApplication(ApplicationOperations.StopOperation operation){
        return applicationManagementService.stop(operation);
    }
}