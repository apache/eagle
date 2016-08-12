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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.entity.FileStatusEntity;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.apache.eagle.security.service.MetadataDaoFactory;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.hdfs.HDFSResourceConstants;
import org.apache.eagle.service.security.hdfs.HDFSResourceSensitivityDataJoiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.service.security.hdfs.HDFSFileSystem;


/**
 * REST Web Service to browse files and Paths in HDFS
 */
@Path(HDFSResourceConstants.HDFS_RESOURCE)
public class HDFSResourceWebResource {
	private static Logger LOG = LoggerFactory.getLogger(HDFSResourceWebResource.class);
	final public static String HDFS_APPLICATION = "HdfsAuditLogApplication";
	private ApplicationEntityService entityService;
	private ISecurityMetadataDAO dao;

	@Inject
	public HDFSResourceWebResource(ApplicationEntityService entityService, Config eagleServerConfig){
		this.entityService = entityService;
		dao = MetadataDaoFactory.getMetadataDAO(eagleServerConfig);
	}

	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public HDFSResourceWebResponse  getHDFSResource( @QueryParam("site") String site , @QueryParam("path") String filePath )
	{
		LOG.info("Starting HDFS Resource Browsing.  Query Parameters ==> Site :"+site+"  Path : "+filePath );
		HDFSResourceWebResponse response = new HDFSResourceWebResponse();
		HDFSResourceWebRequestValidator validator = new HDFSResourceWebRequestValidator();
		List<FileStatusEntity> result = new ArrayList<>();
		List<FileStatus> fileStatuses = null;
		try {
			validator.validate(site, filePath); // First Step would be validating Request
			Map<String, Object> config = getAppConfig(site, HDFS_APPLICATION);
			Configuration conf = convert(config);
			HDFSFileSystem fileSystem = new HDFSFileSystem(conf);
			fileStatuses = fileSystem.browse(filePath);
			// Join with File Sensitivity Info
			HDFSResourceSensitivityDataJoiner joiner = new HDFSResourceSensitivityDataJoiner();
			result = joiner.joinFileSensitivity(site, fileStatuses);
			LOG.info("Successfully browsed files in HDFS .");
		} catch( Exception ex ) {
			response.setException(EagleExceptionWrapper.wrap(ex));
			LOG.error(" Exception When browsing Files for the HDFS Path  :"+filePath+"  " , ex);
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