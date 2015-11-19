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
package org.apache.eagle.service.security.hdfs.resolver;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.eagle.service.security.hdfs.HDFSResourceAccessConfig;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.service.security.hdfs.HDFSFileSystem;
import org.apache.eagle.service.security.hdfs.HDFSResourceConstants;
import org.apache.eagle.service.security.hdfs.HDFSResourceUtils;

/**
 * HDFS Resource Resolver 
 *
 * Generic Resolver Will invoke this HDFS Resolvers 
 * Query HINT : HDFS Resource resolve must be {\"site\":\"${site}\", \"query\"=\"{directory path}\"}
 */
public class HDFSResourceResolver  implements AttributeResolvable<GenericAttributeResolveRequest,String> {
	private final static Logger LOG = LoggerFactory.getLogger(HDFSResourceResolver.class);
	/**
	 * HDFS Resource Resolve API
     *
	 * returns listOfPaths
	 */
	@Override
	public List<String> resolve(GenericAttributeResolveRequest request)
			throws AttributeResolveException {
        List<String> result = new ArrayList<>();
		try {
			HDFSResourceAccessConfig config = HDFSResourceUtils.getConfig(request.getSite().trim());
			HDFSFileSystem fileSystem = new HDFSFileSystem(config.getHdfsEndpoint());
			String query = request.getQuery().trim();
			List<FileStatus> fileStatuses = null;
			if(query.endsWith("/")) {
				fileStatuses =  fileSystem.browse(request.getQuery().trim());
			}
			else{
				Matcher m = Pattern.compile("(.*/)([\\w\\s]+)").matcher(query);
				if(m.find()) {
					List<FileStatus> allFileStatuses = fileSystem.browse(m.group(1));
					fileStatuses = matchAttribute(allFileStatuses, query);
				}
				else {
					throw new BadAttributeResolveRequestException(HDFSResourceConstants.HDFS_RESOURCE_RESOLVE_FORMAT_HINT);
				}
			}
            for(FileStatus status: fileStatuses){
                result.add(status.getPath().toUri().getPath());
            }

			LOG.info("Successfully browsed files in HDFS .");
			return result;
		} catch( Exception e ) {
			LOG.error(" Exception in HDFS Resource Resolver ", e);
			throw new AttributeResolveException(e);
		}
	}
	
	/**
	 * Validate the Passed Request Object
	 * It should have Site Id and File Path 
	 */
	@Override
	public void validateRequest(GenericAttributeResolveRequest request)
			throws BadAttributeResolveRequestException {
        if(LOG.isDebugEnabled()) LOG.debug(" validating HDFS Resource Resolve  request ...");
		String siteId = request.getSite();
		if( null == siteId )
			throw new BadAttributeResolveRequestException(HDFSResourceConstants.HDFS_RESOURCE_RESOLVE_FORMAT_HINT);
		String filePath = request.getQuery();
		if( null == filePath || !filePath.startsWith("/"))
//                || filePath.split("/").length > 1  )
			throw new BadAttributeResolveRequestException(HDFSResourceConstants.HDFS_RESOURCE_RESOLVE_FORMAT_HINT);
		if(LOG.isDebugEnabled()) LOG.debug(" HDFS Resource Resolve request validated successfully...");
	}

	public List<FileStatus> matchAttribute(List<FileStatus> statuses, String target) {
		List<FileStatus> result = new ArrayList<>();
		Pattern pattern = Pattern.compile("^" + target);
		for (FileStatus status : statuses) {
			String path = status.getPath().toUri().getPath();
			if (pattern.matcher(path).find()){
				result.add(status);
			}
		}
		if(result.size() == 0) {
			return statuses;
		}
		return result;
	}
	/**
	 * 
	 */
	@Override
	public Class<GenericAttributeResolveRequest> getRequestClass() {
		return GenericAttributeResolveRequest.class;
	}
}