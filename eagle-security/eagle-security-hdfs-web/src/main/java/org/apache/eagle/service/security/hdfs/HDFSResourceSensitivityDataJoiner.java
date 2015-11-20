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
package org.apache.eagle.service.security.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

import org.apache.eagle.security.hdfs.entity.FileStatusEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS Resource Sensitivity Joiner 
 * 
 * Example.. 
 * If the Directory Path  /user/data has some sensitivity information 
 * this should be verified against with our Sensitivity Map to Join the Type of Sensitivity 
 * the same will be displayed in RED in UI
 */
public class HDFSResourceSensitivityDataJoiner {

	private final static Logger LOG = LoggerFactory.getLogger(HDFSResourceSensitivityDataJoiner.class);
	/**
	 * Checks if the file Path comes under Sensitivity Category if yes append the Sensitivity Type at the End
	 * @param site
	 * @return filePathListWithSensitivityTpe
	 * @throws IOException 
	 */
	
	public List<FileStatusEntity>  joinFileSensitivity( String site , List<FileStatus> fileStatuses ) {		
        List<FileStatusEntity> result = new ArrayList<>();
        HDFSResourceSensitivityService sensitivityService = new HDFSResourceSensitivityService();
        Map<String, String>  sensitivityMap = sensitivityService.getFileSensitivityMapBySite(site);
        LOG.info("Joining Resource with Sensitivity data ..");
		for( FileStatus fileStatus : fileStatuses ) {
            String resource = fileStatus.getPath().toUri().getPath();         
            FileStatusEntity entity;
			try {
				entity = new FileStatusEntity(fileStatus);
				entity.setResource(resource);
	            entity.setSensitiveType(sensitivityMap.get(resource));
	            entity.setChildSensitiveTypes(getChildSensitivityTypes(resource , sensitivityMap ));
				result.add(entity);
			} catch (IOException e) {
				LOG.error(" Exception when joining FileSensitivity .. Error Message : "+e.getMessage());
			}
            
		}
		return result;
	}
		
	/**
	 * Fetch the Sensitivity Types by applying RegExp Pattern 
	 * @param resource
	 * @param sensitivityMap
	 * @return childSensitivityTypes
	 */
	public Set<String>  getChildSensitivityTypes( String resource , Map<String,String> sensitivityMap )
	{
		Set<String>  childSensitiveTypes = new HashSet<String>();
		LOG.info("Fetching Child Seneitivity for the resource : "+resource );
		if (resource == null || resource.isEmpty())
			return childSensitiveTypes;
		Set<String> keySet = sensitivityMap.keySet();
		if(keySet.isEmpty()) return childSensitiveTypes;

		String fullResource = resource + ".*";
		for( String path : keySet ) {
			if(path == null) continue;
			//resource = path.equalsIgnoreCase(resource) ? "" : resource+".*";
			// if( path.matches(resource) ) childSensitivityTypes.add(sensitivityMap.get(path));
			if(!path.equalsIgnoreCase(resource) && path.matches(fullResource)) {
				childSensitiveTypes.add(sensitivityMap.get(path));
			}
		}
		return childSensitiveTypes;
	}
}