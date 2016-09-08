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
package org.apache.eagle.service.security.hdfs.rest;


import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.entity.FileStatusEntity;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.apache.eagle.security.service.MetadataDaoFactory;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.hdfs.HDFSFileSystem;
import org.apache.eagle.service.security.hdfs.HDFSResourceSensitivityDataJoiner;
import org.apache.eagle.service.security.hdfs.MAPRFSResourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST Web Service to browse files and Paths in MAPRFS
 */
@Path(MAPRFSResourceConstants.MAPRFS_RESOURCE)
public class MAPRFSResourceWebResource
{
    private static Logger LOG = LoggerFactory.getLogger(MAPRFSResourceWebResource.class);
    final public static String MAPRFS_APPLICATION = "maprFSAuditLog";
    private ApplicationEntityService entityService;
    private ISecurityMetadataDAO dao;

    @Inject
    public MAPRFSResourceWebResource(ApplicationEntityService entityService, Config eagleServerConfig){
        this.entityService = entityService;
        dao = MetadataDaoFactory.getMetadataDAO(eagleServerConfig);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HDFSResourceWebResponse getHDFSResource(@QueryParam("site") String site , @QueryParam("path") String filePath )
    {
        LOG.info("Starting MAPRFS Resource Browsing.  Query Parameters ==> Site :"+site+"  Path : "+filePath );
        HDFSResourceWebResponse response = new HDFSResourceWebResponse();
        HDFSResourceWebRequestValidator validator = new HDFSResourceWebRequestValidator();
        List<FileStatusEntity> result = new ArrayList<>();
        List<FileStatus> fileStatuses = null;
        try {
            validator.validate(site, filePath); // First Step would be validating Request
            Map<String, Object> config = getAppConfig(site, MAPRFS_APPLICATION);
            Configuration conf = convert(config);
            HDFSFileSystem fileSystem = new HDFSFileSystem(conf);
            fileStatuses = fileSystem.browse(filePath);
            // Join with File Sensitivity Info
            HDFSResourceSensitivityDataJoiner joiner = new HDFSResourceSensitivityDataJoiner(dao);
            result = joiner.joinFileSensitivity(site, fileStatuses);
            LOG.info("Successfully browsed files in MAPRFS .");
        } catch( Exception ex ) {
            response.setException(EagleExceptionWrapper.wrap(ex));
            LOG.error(" Exception When browsing Files for the MAPRFS Path  :"+filePath+"  " , ex);
        }
        response.setObj(result);
        return response;
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
}
