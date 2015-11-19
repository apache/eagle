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
package org.apache.eagle.service.security.hive.dao;

import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.eagle.security.hive.entity.HiveResourceSensitivityAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveSensitivityMetadataDAOImpl implements HiveSensitivityMetadataDAO{
    private static Logger LOG = LoggerFactory.getLogger(HiveSensitivityMetadataDAOImpl.class);

    @Override
    public Map<String, Map<String, String>> getAllHiveSensitivityMap(){
        ListQueryResource resource = new ListQueryResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        ListQueryAPIResponseEntity ret = resource.listQuery("HiveResourceSensitivityService[]{*}", null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<HiveResourceSensitivityAPIEntity> list = (List<HiveResourceSensitivityAPIEntity>) ret.getObj();
        if( list == null )
        	return Collections.emptyMap();
        Map<String, Map<String, String>> res = new HashMap<String, Map<String, String>>();
        
        for(HiveResourceSensitivityAPIEntity entity : list){
            String site = entity.getTags().get("site");
            if(entity.getTags().containsKey("hiveResource")) {
                if(res.get(site) == null){
                    res.put(site, new HashMap<String, String>());
                }
                Map<String, String> resSensitivityMap = res.get(site);
                resSensitivityMap.put(entity.getTags().get("hiveResource"), entity.getSensitivityType());
            }
            else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("An invalid sensitivity entity is detected" + entity);
                }
            }
        }
        return res;
    }

    @Override
    public Map<String, String> getHiveSensitivityMap(String site){
        ListQueryResource resource = new ListQueryResource();
        String queryFormat = "HiveResourceSensitivityService[@site=\"%s\"]{*}";
        ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, site), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<HiveResourceSensitivityAPIEntity> list = (List<HiveResourceSensitivityAPIEntity>) ret.getObj();
        if( list == null )
        	return Collections.emptyMap();
        Map<String, String> resSensitivityMap = new HashMap<String, String>();
        for(HiveResourceSensitivityAPIEntity entity : list){
            if(entity.getTags().containsKey("hiveResource")) {
                resSensitivityMap.put(entity.getTags().get("hiveResource"), entity.getSensitivityType());
            }
        }
        return resSensitivityMap;
    }
    
}
