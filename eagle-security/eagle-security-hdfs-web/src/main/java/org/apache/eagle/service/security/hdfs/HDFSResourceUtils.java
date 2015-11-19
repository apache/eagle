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

import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.hadoop.fs.FileStatus;

import org.apache.eagle.alert.entity.AlertDataSourceEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Util API which has common methods and Service calls API
 */
public class HDFSResourceUtils {
		
	@SuppressWarnings("unchecked")
	public static HDFSResourceAccessConfig  getConfig(String siteId ) throws Exception
	{
		ListQueryResource resource = new ListQueryResource();
		String queryFormat = "AlertDataSourceService[@dataSource=\""+HDFSResourceConstants.HDFS_DATA_SOURCE+"\" AND @site=\"%s\"]{*}";
		ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, siteId), null, null,Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null);
		List<AlertDataSourceEntity> list = (List<AlertDataSourceEntity>) ret.getObj();
		if (list == null || list.size() == 0)
			throw new Exception("Config is empty for site " + siteId +".");
	    
		ObjectMapper mapper = new ObjectMapper();
		HDFSResourceAccessConfig config = mapper.readValue(list.get(0).getConfig(), HDFSResourceAccessConfig.class);				
		return config;
	}	
	
	/**
	 * Not Null String Check Method 
	 * @param input
	 * @return
	 */
	
	public static boolean isNullOrEmpty( String input )
	{
		if( null == input ||  input.length() <= 0 )
			return true;
			
		return false;
	}
	
	/**
	 * Filter Only directories 
	 * @param fileStatuses
	 * @return
	 */
	public static List<String> filterDirectories( List<FileStatus> fileStatuses)
	{
		List<String> directories  = new ArrayList<>();
		for(FileStatus fileStatus : fileStatuses )
		{
			if( fileStatus.isDirectory() )
				directories.add(fileStatus.getPath().toUri().getPath());
			
		}
		return directories;
	}
	
	
	/**
	 * Returns the filtered data by applyin RegExp
	 * @param regExp
	 * @param data
	 * @return
	 */
	public static List<String> filterCollectionUsingRegEx( String regExp , List<String> data )
	{
		 // No regular expressions filtering needed
        if (regExp == null || regExp.isEmpty()) {
            return data;
        }
        
        List<String> filteredElems = new ArrayList<String>();
        for( String path : data )
        {
        	if( path.matches(regExp) )
        			filteredElems.add(path);
        }       
		return filteredElems;
	}
}
