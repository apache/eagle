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


import com.typesafe.config.*;
import org.apache.eagle.alert.entity.SiteApplicationServiceEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class MetadataAccessConfigRepo {
    private static Logger LOG = LoggerFactory.getLogger(MetadataAccessConfigRepo.class);

    public Config getConfig(String application, String siteId) throws Exception {

        GenericEntityServiceResource resource = new GenericEntityServiceResource();
        String queryFormat = Constants.SITE_APPLICATION_SERVICE_ENDPOINT_NAME + "[@application=\"%s\" AND @site=\"%s\"]{*}";
        GenericServiceAPIResponseEntity ret = resource.search(String.format(queryFormat, application, siteId), null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);
        List<SiteApplicationServiceEntity> list = (List<SiteApplicationServiceEntity>) ret.getObj();
        if (list == null || list.size() == 0)
            throw new Exception("Config is empty for site=" + siteId +" application=" + application + ".");
        String originalConfigStr = list.get(0).getConfig();
        ConfigParseOptions options = ConfigParseOptions.defaults()
                .setSyntax(ConfigSyntax.PROPERTIES)
                .setAllowMissing(false);
        Config originalConfig = ConfigFactory.parseString(originalConfigStr, options);
        if(!originalConfig.hasPath(EagleConfigConstants.WEB_CONFIG)) {
            throw new Exception("Fail to get WEB_CONFIG configurations for data classification");
        }
        return originalConfig.getConfig(EagleConfigConstants.WEB_CONFIG);
    }

    public Configuration convert(Config originalConfig) throws Exception {
        Configuration config = new Configuration();
        for (Map.Entry<String, ConfigValue> entry : originalConfig.entrySet()) {
            config.set(entry.getKey().toString(), entry.getValue().unwrapped().toString());
        }
        return config;
    }
}