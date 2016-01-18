/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.dsl.rest;


/*@Path(AppConstants.APP_DEFINITION_PATH)
public class AppDefinitionResource {
    private static Logger LOG = LoggerFactory.getLogger(AppDefinitionResource.class);
    private final static AppEntityDao dao = new AppEntityDaoImpl2();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity<AppDefinitionEntity>  getAllAppDefinitions(@QueryParam("query") String query,
                                                             @QueryParam("pageSize") int pageSize) {
        GenericServiceAPIResponseEntity<AppDefinitionEntity> response = dao.search(query, pageSize);
        return response;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity  createAppDefinition(List<AppDefinitionEntity> entities) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.create(entities, AppConstants.APP_DEFINITION_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity  updateAppDefinition(List<AppDefinitionEntity> entities) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.update(entities, AppConstants.APP_DEFINITION_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }

    @POST
    @Path(AppConstants.DELETE_ENTITIES_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteAppDefinition(List<AppDefinitionEntity> entities) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.deleteByEntities(entities, AppConstants.APP_DEFINITION_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }

}*/
