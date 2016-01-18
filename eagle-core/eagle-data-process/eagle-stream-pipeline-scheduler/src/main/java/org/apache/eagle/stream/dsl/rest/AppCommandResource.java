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


/*
@Path(AppConstants.APP_COMMAND_PATH)
public class AppCommandResource {
    private static Logger LOG = LoggerFactory.getLogger(AppCommandResource.class);
    private final static AppEntityDao dao = new AppEntityDaoImpl2();

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity  createAppCommand(List<AppCommandEntity> entities) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.create(entities, AppConstants.APP_COMMAND_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity<AppCommandEntity> getAllAppCommands(@QueryParam("query") String query,
                                                             @QueryParam("pageSize") int pageSize) {
        GenericServiceAPIResponseEntity<AppCommandEntity> response = dao.search(query, pageSize);
        return response;
    }


    @POST
    @Path(AppConstants.DELETE_ENTITIES_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteAppCommand(List<AppCommandEntity> entities) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.deleteByEntities(entities, AppConstants.APP_COMMAND_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteAppCommandById(List<String> ids) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            response = dao.deleteByIds(ids, AppConstants.APP_COMMAND_SERVICE);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }
}
*/
