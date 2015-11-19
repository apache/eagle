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

package org.apache.eagle.service.security.hbase;


import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.security.hbase.HbaseResourceSensitivityAPIEntity;
import org.apache.eagle.service.generic.ListQueryResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseSensitivityResourceService {
    private static Logger LOG = LoggerFactory.getLogger(HbaseSensitivityResourceService.class);

    public Map<String, Map<String, String>> getAllHbaseSensitivityMap(){
        ListQueryResource resource = new ListQueryResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        ListQueryAPIResponseEntity ret = resource.listQuery("HbaseResourceSensitivityService[]{*}", null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<HbaseResourceSensitivityAPIEntity> list = (List<HbaseResourceSensitivityAPIEntity>) ret.getObj();
        if( list == null )
            return Collections.emptyMap();
        Map<String, Map<String, String>> res = new HashMap<String, Map<String, String>>();

        for(HbaseResourceSensitivityAPIEntity entity : list){
            String site = entity.getTags().get("site");
            if(entity.getTags().containsKey("hbaseResource")) {
                if(res.get(site) == null){
                    res.put(site, new HashMap<String, String>());
                }
                Map<String, String> resSensitivityMap = res.get(site);
                resSensitivityMap.put(entity.getTags().get("hbaseResource"), entity.getSensitivityType());
            }
            else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("An invalid sensitivity entity is detected" + entity);
                }
            }
        }
        return res;
    }

    public Map<String, String> getHbaseSensitivityMap(String site){
        ListQueryResource resource = new ListQueryResource();
        String queryFormat = "HbaseResourceSensitivityService[@site=\"%s\"]{*}";
        ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, site), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<HbaseResourceSensitivityAPIEntity> list = (List<HbaseResourceSensitivityAPIEntity>) ret.getObj();
        if( list == null )
            return Collections.emptyMap();
        Map<String, String> resSensitivityMap = new HashMap<String, String>();
        for(HbaseResourceSensitivityAPIEntity entity : list){
            if(entity.getTags().containsKey("hbaseResource")) {
                resSensitivityMap.put(entity.getTags().get("hbaseResource"), entity.getSensitivityType());
            }
        }
        return resSensitivityMap;
    }
}
