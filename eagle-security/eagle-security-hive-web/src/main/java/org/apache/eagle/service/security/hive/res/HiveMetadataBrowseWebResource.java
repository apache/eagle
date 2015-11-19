/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.security.hive.res;

import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.security.hive.entity.HiveResourceEntity;
import org.apache.eagle.service.security.hive.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.regex.Pattern;

@Path("/hiveResource")
public class HiveMetadataBrowseWebResource {
    private static Logger LOG = LoggerFactory.getLogger(HiveMetadataBrowseWebResource.class);
    private HiveSensitivityMetadataDAOImpl dao = new HiveSensitivityMetadataDAOImpl();
    private Map<String, Map<String, String>> maps = dao.getAllHiveSensitivityMap();

    @Path("/databases")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HiveMetadataBrowseWebResponse getDatabases(@QueryParam("site") String site){
        // delegate to HiveMetadataDAO
        HiveMetadataBrowseWebResponse response = new HiveMetadataBrowseWebResponse();
        List<String> databases = null;
        List<HiveResourceEntity> values = new ArrayList<>();
        try {
            HiveMetadataAccessConfig config = new HiveMetadataAccessConfigDAOImpl().getConfig(site);
            HiveMetadataDAO dao = new HiveMetadataDAOFactory().getHiveMetadataDAO(config);
            databases = dao.getDatabases();
        } catch(Exception ex){
            LOG.error("fail getting databases", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }

        if(databases != null) {
            String resource = null;
            for (String db : databases) {
                resource = String.format("/%s", db);
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, resource, childSensitiveTypes);
                values.add(new HiveResourceEntity(resource, db, null, null, senstiveType, childSensitiveTypes));
            }
        }
        response.setObj(values);
        return response;
    }

    @Path("/tables")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HiveMetadataBrowseWebResponse getTables(@QueryParam("site") String site, @QueryParam("database") String database){
        // delegate to HiveMetadataDAO
        HiveMetadataBrowseWebResponse response = new HiveMetadataBrowseWebResponse();
        List<String> tables = null;
        List<HiveResourceEntity> values = new ArrayList<>();
        try {
            HiveMetadataAccessConfig config = new HiveMetadataAccessConfigDAOImpl().getConfig(site);
            HiveMetadataDAO dao = new HiveMetadataDAOFactory().getHiveMetadataDAO(config);
            tables = dao.getTables(database);
        }catch(Exception ex){
            LOG.error("fail getting databases", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }
        if(tables != null) {
            String resource = null;
            for (String table : tables) {
                resource = String.format("/%s/%s", database, table);
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, resource, childSensitiveTypes);
                values.add(new HiveResourceEntity(resource, database, table, null, senstiveType, childSensitiveTypes));
            }
        }
        response.setObj(values);
        return response;
    }

    @Path("/columns")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HiveMetadataBrowseWebResponse getColumns(@QueryParam("site") String site, @QueryParam("database") String database, @QueryParam("table") String table){
        // delegate to HiveMetadataDAO
        HiveMetadataBrowseWebResponse response = new HiveMetadataBrowseWebResponse();
        List<String> columns = null;
        List<HiveResourceEntity> values = new ArrayList<>();
        try {
            HiveMetadataAccessConfig config = new HiveMetadataAccessConfigDAOImpl().getConfig(site);
            HiveMetadataDAO dao = new HiveMetadataDAOFactory().getHiveMetadataDAO(config);
            columns = dao.getColumns(database, table);
        }catch(Exception ex){
            LOG.error("fail getting databases", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }
        if(columns != null) {
            String resource = null;
            for (String col : columns) {
                resource = String.format("/%s/%s/%s", database, table, col);
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, resource, childSensitiveTypes);
                values.add(new HiveResourceEntity(resource, database, table, col, senstiveType, childSensitiveTypes));
            }
        }
        response.setObj(values);
        return response;
    }

    String checkSensitivity(String site, String resource, Set<String> childSensitiveTypes) {
        String sensitiveType = null;
        if (maps != null && maps.get(site) != null) {
            Map<String, String> map = maps.get(site);
            for (String r : map.keySet()) {
                Pattern pattern = Pattern.compile("^" + resource);
                boolean isMatched = Pattern.matches(r, resource);
                if (isMatched) {
                    sensitiveType = map.get(r);
                }
                else if (pattern.matcher(r).find()){
                    childSensitiveTypes.add(map.get(r));
                }
            }
        }
        return sensitiveType;
    }

}
