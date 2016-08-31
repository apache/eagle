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


import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.service.ApplicationManagementService;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.resource.RESTResponse;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

import static org.apache.eagle.metadata.utils.HttpRequest.httpGetWithoutCredentials;

@Path("/apps")
public class ApplicationResource {
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationResource.class);

    private final ApplicationProviderService providerService;
    private final ApplicationManagementService applicationManagementService;
    private final ApplicationEntityService entityService;

    private ApplicationEntity.Status extractStatus(JSONObject response, String appUuid) {
        ApplicationEntity.Status status = ApplicationEntity.Status.UNINSTALLED;
        try {
            JSONArray list = (JSONArray) response.get("topologies");
            for(int i = 0; i< list.length();i++ ){
                JSONObject topology = (JSONObject) list.get(i);
                if(topology.getString("id").equals(appUuid)) {
                    status = ApplicationEntity.Status.valueOf((topology.getString("status")).toUpperCase());
                    return status;
                }
            }
        } catch (JSONException e){
            LOG.info("application is not running or not existed", e);
        } catch (Exception e){
            LOG.info("failed to get application status from storm UI rest service", e);
        }

        return status;
    }

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
    public RESTResponse<Collection<ApplicationDesc>> getApplicationDescs(){
        return RESTResponse.async(providerService::getApplicationDescs).get();
    }

    @GET
    @Path("/providers/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationDesc> getApplicationDescByType(@PathParam("type") String type){
        return RESTResponse.async(()->providerService.getApplicationDescByType(type)).get();
    }

    @PUT
    @Path("/providers/reload")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationDesc>> reloadApplicationDescs(){
        return RESTResponse.<Collection<ApplicationDesc>>async((response)-> {
            providerService.reload();
            response.message("Successfully reload application providers");
            response.data(providerService.getApplicationDescs());
        }).get();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<ApplicationEntity>> getApplicationEntities(@QueryParam("siteId") String siteId){
        return RESTResponse.async(()-> {
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
    public RESTResponse<ApplicationEntity> getApplicationEntityByUUID(@PathParam("appUuid") String appUuid){
        return RESTResponse.async(()->entityService.getByUUID(appUuid)).get();
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<ApplicationEntity.Status> getApplicationStatusByUUID(@QueryParam("appUuid") String appUuid){
        return RESTResponse.async(
                ()->{
                    Config config = ConfigFactory.load();
                    String host = config.getString("application.storm.uiHost");
                    String port = config.getString("application.storm.uiPort");
                    JSONObject response = httpGetWithoutCredentials("http://" + host + ":" + port + "/api/v1/topology/summary");
                    return extractStatus(response, appUuid);
                }
        ).get();
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
        return RESTResponse.<ApplicationEntity>async((response)-> {
            ApplicationEntity entity = applicationManagementService.install(operation);
            response.message("Successfully installed application "+operation.getAppType()+" onto site "+operation.getSiteId());
            response.data(entity);
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
        return RESTResponse.<Void>async((response)-> {
            ApplicationEntity entity = applicationManagementService.uninstall(operation);
            response.success(true).message("Successfully uninstalled application "+entity.getUuid());
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
        return RESTResponse.<Void>async((response)-> {
            ApplicationEntity entity = applicationManagementService.start(operation);
            response.success(true).message("Successfully started application "+entity.getUuid());
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
        return RESTResponse.<Void>async((response)-> {
            ApplicationEntity entity = applicationManagementService.stop(operation);
            response.success(true).message("Successfully stopped application "+entity.getUuid());
        }).get();
    }
}