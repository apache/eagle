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

import org.apache.eagle.alert.entity.AlertDataSourceEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.service.generic.ListQueryResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataAccessConfigDAOImpl implements HiveMetadataAccessConfigDAO {
    private static Logger LOG = LoggerFactory.getLogger(HiveMetadataAccessConfigDAOImpl.class);

    // site to HiveMetadataAccessConfig
    @Override
    public Map<String, HiveMetadataAccessConfig> getAllConfigs() throws Exception{
        ListQueryResource resource = new ListQueryResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        ListQueryAPIResponseEntity ret = resource.listQuery("AlertDataSourceService[@dataSource=\"hiveQueryLog\"]{*}", null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<AlertDataSourceEntity> list = (List<AlertDataSourceEntity>) ret.getObj();
        Map<String, HiveMetadataAccessConfig> siteHiveConfigs = new HashMap<String, HiveMetadataAccessConfig>();
        for(AlertDataSourceEntity hiveDataSource : list){
            siteHiveConfigs.put(hiveDataSource.getTags().get("site"), convert(hiveDataSource.getConfig()));
        }
        return siteHiveConfigs;
    }

    private HiveMetadataAccessConfig convert(String config){
        ObjectMapper mapper = new ObjectMapper();
        HiveMetadataAccessConfig c = null;
        try {
            c = mapper.readValue(config, HiveMetadataAccessConfig.class);
        }catch(Exception ex){
            LOG.error("config block could be broken", ex);
            throw new BadHiveMetadataAccessConfigException(ex);
        }
        return c;
    }

    // HiveMetadataAccessConfig for one site
    @Override
    public HiveMetadataAccessConfig getConfig(String site) throws Exception{
        ListQueryResource resource = new ListQueryResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        String queryFormat = "AlertDataSourceService[@dataSource=\"hiveQueryLog\" AND @site=\"%s\"]{*}";
        ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, site), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<AlertDataSourceEntity> list = (List<AlertDataSourceEntity>) ret.getObj();
        if(list == null || list.size() ==0)
            throw new BadHiveMetadataAccessConfigException("config is empty for site " + site);
        return convert(list.get(0).getConfig());
    }
}
