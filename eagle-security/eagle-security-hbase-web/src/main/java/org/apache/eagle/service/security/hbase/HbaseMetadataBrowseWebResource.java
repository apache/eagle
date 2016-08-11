/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.service.security.hbase;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.entity.HbaseResourceEntity;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataDAOImpl;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.regex.Pattern;

@Path("/hbaseResource")
public class HbaseMetadataBrowseWebResource {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMetadataBrowseWebResource.class);
    private MetadataAccessConfigRepo repo = new MetadataAccessConfigRepo();
    final public static String HBASE_APPLICATION = "HBaseAuditLogApplication";

    private ApplicationEntityService entityService;
    private ISecurityMetadataDAO dao;

    @Inject
    public HbaseMetadataBrowseWebResource(ApplicationEntityService entityService, ISecurityMetadataDAO metadataDAO){
        this.entityService = entityService;
        this.dao = metadataDAO;
    }

    private Map<String, Map<String, String>> getAllSensitivities(){
        Map<String, Map<String, String>> all = new HashMap<>();
        Collection<HBaseSensitivityEntity> entities = dao.listHBaseSensitivies();
        for(HBaseSensitivityEntity entity : entities){
            if(!all.containsKey(entity.getSite())){
                all.put(entity.getSite(), new HashMap<>());
            }
            all.get(entity.getSite()).put(entity.getHbaseResource(), entity.getSensitivityType());
        }
        return all;
    }

    private Map<String, Object> getAppConfig(String site, String appType){
        ApplicationEntity entity = entityService.getBySiteIdAndAppType(site, appType);
        return entity.getConfiguration();
    }


    private Configuration convert(Map<String, Object> originalConfig) throws Exception {
        Configuration config = new Configuration();
        for (Map.Entry<String, Object> entry : originalConfig.entrySet()) {
            config.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return config;
    }

    @Path("/namespaces")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HbaseMetadataBrowseWebResponse getNamespace(@QueryParam("site") String site) {
        List<String> namespaces = null;
        List<HbaseResourceEntity> values = new ArrayList<>();
        HbaseMetadataBrowseWebResponse response = new HbaseMetadataBrowseWebResponse();
        try {
            Map<String, Object> config = getAppConfig(site, HBASE_APPLICATION);
            Configuration conf = convert(config);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(conf);
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
            Map<String, Object> config = getAppConfig(site, HBASE_APPLICATION);
            Configuration conf = convert(config);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(conf);
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
            Config config = repo.getConfig(HBASE_APPLICATION, site);
            Configuration conf = repo.convert(config);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(conf);
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
        Map<String, Map<String, String>> maps = getAllSensitivities();
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
