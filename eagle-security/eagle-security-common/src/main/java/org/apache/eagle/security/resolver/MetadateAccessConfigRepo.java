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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.security.resolver;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.alert.entity.AlertDataSourceEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.service.generic.ListQueryResource;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MetadateAccessConfigRepo {
    private static Logger LOG = LoggerFactory.getLogger(MetadateAccessConfigRepo.class);

    public Configuration getConfig(String datasource, String siteId) throws Exception {

        ListQueryResource resource = new ListQueryResource();
        String queryFormat = "AlertDataSourceService[@dataSource=\"%s\" AND @site=\"%s\"]{*}";
        ListQueryAPIResponseEntity ret = resource.listQuery(String.format(queryFormat, datasource, siteId), null, null,Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null);
        List<AlertDataSourceEntity> list = (List<AlertDataSourceEntity>) ret.getObj();
        if (list == null || list.size() == 0)
            throw new Exception("Config is empty for site=" + siteId +" dataSource=" + datasource + ".");

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> configMap = mapper.readValue(list.get(0).getConfig(), Map.class);
        return convert(configMap);
    }

    private Configuration convert(Map<String, String> configMap) throws Exception {
        Configuration config = new Configuration();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            config.set(entry.getKey(), entry.getValue());
        }
        return config;
    }

}
