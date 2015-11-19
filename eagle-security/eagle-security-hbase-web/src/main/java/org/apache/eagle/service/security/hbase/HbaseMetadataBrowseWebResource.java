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
package org.apache.eagle.service.security.hbase;

import org.apache.eagle.security.hbase.HbaseResourceEntity;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataAccessConfig;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataAccessConfigDAOImpl;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.regex.Pattern;

@Path("/hbaseResource")
public class HbaseMetadataBrowseWebResource {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMetadataBrowseWebResource.class);
    private HbaseSensitivityResourceService dao = new HbaseSensitivityResourceService();
    private Map<String, Map<String, String>> maps = dao.getAllHbaseSensitivityMap();

    @Path("/namespaces")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HbaseMetadataBrowseWebResponse getNamespace(@QueryParam("site") String site) {
        List<String> namespaces = null;
        List<HbaseResourceEntity> values = new ArrayList<>();
        HbaseMetadataBrowseWebResponse response = new HbaseMetadataBrowseWebResponse();
        try {
            HbaseMetadataAccessConfig config = new HbaseMetadataAccessConfigDAOImpl().getConfig(site);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(config);
            namespaces = dao.getNamespaces();

        } catch (Exception e) {
            e.printStackTrace();
        }
        if(namespaces != null) {
            for (String ns : namespaces) {
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, ns, childSensitiveTypes);
                values.add(new HbaseResourceEntity(ns, ns, null, null, senstiveType, childSensitiveTypes));
            }
        }
        response.setObj(values);
        return response;
    }

    @Path("/tables")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HbaseMetadataBrowseWebResponse getTables(@QueryParam("site") String site, @QueryParam("namespace") String namespace){
        // delegate to HiveMetadataDAO
        HbaseMetadataBrowseWebResponse response = new HbaseMetadataBrowseWebResponse();
        List<String> tables = null;
        List<HbaseResourceEntity> values = new ArrayList<>();
        try {
            HbaseMetadataAccessConfig config = new HbaseMetadataAccessConfigDAOImpl().getConfig(site);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(config);
            tables = dao.getTables(namespace);
        }catch(Exception ex){
            LOG.error("fail getting databases", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }
        if(tables != null) {
            String resource = null;
            for (String table : tables) {
                resource = String.format("%s:%s", namespace, table);
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, resource, childSensitiveTypes);
                values.add(new HbaseResourceEntity(resource, namespace, table, null, senstiveType, childSensitiveTypes));
            }
        }
        response.setObj(values);
        return response;
    }

    @Path("/columns")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HbaseMetadataBrowseWebResponse getColumns(@QueryParam("site") String site, @QueryParam("namespace") String namespace, @QueryParam("table") String table){
        // delegate to HiveMetadataDAO
        HbaseMetadataBrowseWebResponse response = new HbaseMetadataBrowseWebResponse();
        List<String> columns = null;
        List<HbaseResourceEntity> values = new ArrayList<>();
        try {
            HbaseMetadataAccessConfig config = new HbaseMetadataAccessConfigDAOImpl().getConfig(site);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(config);
            String tableName = String.format("%s:%s", namespace, table);
            columns = dao.getColumnFamilies(tableName);
        }catch(Exception ex){
            LOG.error("fail getting databases", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }
        if(columns != null) {
            String resource = null;
            for (String col : columns) {
                resource = String.format("%s:%s:%s", namespace, table, col);
                Set<String> childSensitiveTypes = new HashSet<>();
                String senstiveType = checkSensitivity(site, resource, childSensitiveTypes);
                values.add(new HbaseResourceEntity(resource, namespace, table, col, senstiveType, childSensitiveTypes));
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
