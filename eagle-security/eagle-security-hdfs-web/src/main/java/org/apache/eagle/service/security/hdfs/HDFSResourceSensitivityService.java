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

import java.util.*;

import org.apache.eagle.security.service.HdfsSensitivityEntity;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class Queries HDFS file Sensitivity type
 */
public class HDFSResourceSensitivityService {
    private static Logger LOG = LoggerFactory.getLogger(HDFSResourceSensitivityService.class);
    private ISecurityMetadataDAO dao;
    public HDFSResourceSensitivityService(ISecurityMetadataDAO dao){
        this.dao = dao;
    }

    /**
     * Returns all File Sensitivity Entries
     * @return
     */
    public Map<String, Map<String, String>>  getAllFileSensitivityMap()
    {
        Collection<HdfsSensitivityEntity> list = dao.listHdfsSensitivities();
        if( list == null )
            return Collections.emptyMap();
        Map<String, Map<String, String>> fileSensitivityMap = new HashMap <> ();
        Map<String, String>  filedirMap = null;
        for (HdfsSensitivityEntity fileSensitivityObj : list )
        {
            String siteId = fileSensitivityObj.getSite();
            if(fileSensitivityObj.getFiledir() != null) {
                filedirMap = fileSensitivityMap.get(siteId);
                if(!fileSensitivityMap.containsKey(siteId))
                    filedirMap = new HashMap <String, String> ();
                filedirMap.put(fileSensitivityObj.getFiledir(), fileSensitivityObj.getSensitivityType());
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
    public Map<String, String> getFileSensitivityMapBySite ( String site )
    {
        Collection<HdfsSensitivityEntity> list = dao.listHdfsSensitivities();
        if( list == null )
            return Collections.emptyMap();
        Map<String, String>  fileSensitivityMap = new HashMap <String, String> ();
        for ( HdfsSensitivityEntity fileSensitivityObj : list ) {
            if (fileSensitivityObj.getFiledir() != null) {
                fileSensitivityMap.put(fileSensitivityObj.getFiledir(), fileSensitivityObj.getSensitivityType());
            }
        }
        return fileSensitivityMap;
    }

    /**
     * Returns File Sensitivity Info for a Specific  and Passed Path
     * @param site
     * @return fileSensitivityMap
     */
    public Collection<HdfsSensitivityEntity>  filterSensitivity ( String site , String resourceFilter )
    {
        Collection<HdfsSensitivityEntity> list = dao.listHdfsSensitivities();
        if( list == null )
            return Collections.emptyList();
        return list;
    }
}