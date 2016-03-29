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

import com.typesafe.config.Config;
import org.apache.eagle.alert.entity.AlertDataSourceEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.eagle.service.generic.ListQueryResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataAccessConfigDAOImpl implements HiveMetadataAccessConfigDAO {
    private static Logger LOG = LoggerFactory.getLogger(HiveMetadataAccessConfigDAOImpl.class);

    // HiveMetadataAccessConfig for one site
    @Override
    public HiveMetadataAccessConfig getConfig(String site) throws Exception{
        MetadataAccessConfigRepo repo = new MetadataAccessConfigRepo();
        Config config = repo.getConfig("hiveQueryLog", site);
        return HiveMetadataAccessConfig.config2Entity(config);
    }
}