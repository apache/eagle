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
package org.apache.eagle.service.security.oozie.dao;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.security.entity.OozieResourceSensitivityAPIEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OozieSensitivityMetadataDAOImpl implements OozieSensitivityMetadataDAO {
    private static Logger LOG = LoggerFactory.getLogger(OozieSensitivityMetadataDAOImpl.class);

    @Override
    public Map<String, Map<String, String>> getAllOozieSensitivityMap() {
        GenericEntityServiceResource resource = new GenericEntityServiceResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        GenericServiceAPIResponseEntity ret = resource.search("OozieResourceSensitivityService[]{*}", null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null, false);
        List<OozieResourceSensitivityAPIEntity> list = (List<OozieResourceSensitivityAPIEntity>) ret.getObj();
        if (list == null) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> res = new HashMap<String, Map<String, String>>();

        for (OozieResourceSensitivityAPIEntity entity : list) {
            String site = entity.getTags().get("site");
            if (entity.getTags().containsKey("oozieResource")) {
                if (res.get(site) == null) {
                    res.put(site, new HashMap<String, String>());
                }
                Map<String, String> resSensitivityMap = res.get(site);
                resSensitivityMap.put(entity.getTags().get("oozieResource"), entity.getSensitivityType());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("An invalid sensitivity entity is detected" + entity);
                }
            }
        }
        return res;
    }

    @Override
    public Map<String, String> getOozieSensitivityMap(String site) {
        GenericEntityServiceResource resource = new GenericEntityServiceResource();
        String queryFormat = "OozieResourceSensitivityService[@site=\"%s\"]{*}";
        GenericServiceAPIResponseEntity ret = resource.search(String.format(queryFormat, site), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null, false);
        List<OozieResourceSensitivityAPIEntity> list = (List<OozieResourceSensitivityAPIEntity>) ret.getObj();
        if (list == null) {
            return Collections.emptyMap();
        }
        Map<String, String> resSensitivityMap = new HashMap<String, String>();
        for (OozieResourceSensitivityAPIEntity entity : list) {
            if (entity.getTags().containsKey("oozieResource")) {
                resSensitivityMap.put(entity.getTags().get("oozieResource"), entity.getSensitivityType());
            }
        }
        return resSensitivityMap;
    }

}
