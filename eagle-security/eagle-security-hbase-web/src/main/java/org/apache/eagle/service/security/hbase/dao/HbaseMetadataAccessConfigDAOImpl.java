/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.service.security.hbase.dao;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.alert.entity.AlertDataSourceEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.security.util.BadMetadataAccessConfigException;
import org.apache.eagle.service.generic.ListQueryResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HbaseMetadataAccessConfigDAOImpl {
    private static Logger LOG = LoggerFactory.getLogger(HbaseMetadataAccessConfigDAOImpl.class);

    private HbaseMetadataAccessConfig convert(String config){
        ObjectMapper mapper = new ObjectMapper();
        HbaseMetadataAccessConfig c = null;
        try {
            c = mapper.readValue(config, HbaseMetadataAccessConfig.class);
        }catch(Exception ex){
            LOG.error("config block could be broken", ex);
            throw new BadMetadataAccessConfigException(ex);
        }
        return c;
    }


    public HbaseMetadataAccessConfig getConfig(String site) throws Exception{
        ListQueryResource resource = new ListQueryResource();
        /* parameters are: query, startTime, endTime, pageSzie, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing,
        * parallel, metricName*/
        String queryFormat = "AlertDataSourceService[@dataSource=\"hbaseSecurityLog\" AND @site=\"%s\"]{*}";
        ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, site), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false,
                0, null);
        List<AlertDataSourceEntity> list = (List<AlertDataSourceEntity>) ret.getObj();
        if(list == null || list.size() ==0)
            throw new BadMetadataAccessConfigException("config is empty for site " + site);
        return convert(list.get(0).getConfig());
    }

}
