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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.eagle.security.hdfs.entity.FileSensitivityAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class Queries HDFS file Sensitivity type 
 */
public class HDFSResourceSensitivityService {
	private static Logger LOG = LoggerFactory.getLogger(HDFSResourceSensitivityService.class);

	/**
	 * Returns all File Sensitivity Entries 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, String>>  getAllFileSensitivityMap()
	{
		  ListQueryResource resource = new ListQueryResource();
		  ListQueryAPIResponseEntity ret = resource.listQuery("FileSensitivityService[]{*}", null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,0, null);
	      List<FileSensitivityAPIEntity> list = (List<FileSensitivityAPIEntity>) ret.getObj();
	      if( list == null )
	        	return Collections.emptyMap();
	      Map<String, Map<String, String>> fileSensitivityMap = new HashMap <String, Map<String, String>> ();
	      Map<String, String>  filedirMap = null;
	      for ( FileSensitivityAPIEntity fileSensitivityObj : list )
	      {
	    	  String siteId = fileSensitivityObj.getTags().get("site");
			  if(fileSensitivityObj.getTags().containsKey("filedir")) {
				  filedirMap = fileSensitivityMap.get(siteId);
				  if(!fileSensitivityMap.containsKey(siteId))
					  filedirMap = new HashMap <String, String> ();
				  filedirMap.put(fileSensitivityObj.getTags().get("filedir"), fileSensitivityObj.getSensitivityType());
				  fileSensitivityMap.put(siteId, filedirMap);
			  }
	      }		  
		return fileSensitivityMap;
	}
	
	/**
	 * Returns File Sensitivity Info for a Specific SITE Id
	 * @param site
	 * @return fileSensitivityMap
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> getFileSensitivityMapBySite ( String site )
	{
		ListQueryResource resource = new ListQueryResource();
		ListQueryAPIResponseEntity ret = resource.listQuery(String.format("FileSensitivityService[@site=\"%s\"]{*}", site ), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,0, null);
	    List<FileSensitivityAPIEntity> list = (List<FileSensitivityAPIEntity>) ret.getObj();	    
	    if( list == null )
        	return Collections.emptyMap();
	    Map<String, String>  fileSensitivityMap = new HashMap <String, String> ();	
	    for ( FileSensitivityAPIEntity fileSensitivityObj : list ) {
			if (fileSensitivityObj.getTags().containsKey("filedir")) {
				fileSensitivityMap.put(fileSensitivityObj.getTags().get("filedir"), fileSensitivityObj.getSensitivityType());
			}
		}
	    return fileSensitivityMap;
	}
		
	/**
	 * Returns File Sensitivity Info for a Specific  and Passed Path
	 * @param site
	 * @return fileSensitivityMap
	 */
	@SuppressWarnings("unchecked")
	public List<FileSensitivityAPIEntity>  filterSensitivity ( String site , String resourceFilter )
	{
		ListQueryResource resource = new ListQueryResource();
		ListQueryAPIResponseEntity ret = resource.listQuery(String.format("FileSensitivityService[@site=\"%s\"]{@filedir="+resourceFilter+".*}", site ), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,0, null);
	    List<FileSensitivityAPIEntity> list = (List<FileSensitivityAPIEntity>) ret.getObj();
	    if( list == null )
        	return Collections.emptyList();	    
	    return list;
	}	
}
