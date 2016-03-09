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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.hdfs.HDFSResourceAccessConfig;
import org.apache.eagle.service.security.hdfs.HDFSResourceConstants;
import org.apache.eagle.service.security.hdfs.HDFSResourceSensitivityDataJoiner;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.service.security.hdfs.HDFSFileSystem;
import org.apache.eagle.service.security.hdfs.HDFSResourceUtils;
import org.apache.eagle.security.hdfs.entity.FileStatusEntity;


/**
 * REST Web Service to browse files and Paths in HDFS
 */
@Path(HDFSResourceConstants.HDFS_RESOURCE)
public class HDFSResourceWebResource 
{
	private static Logger LOG = LoggerFactory.getLogger(HDFSResourceWebResource.class);
	
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
			HDFSResourceAccessConfig config = HDFSResourceUtils.getConfig(site);
			HDFSFileSystem fileSystem = new HDFSFileSystem(config.getHdfsEndpoint());
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
}
