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

package org.apache.eagle.service.security.hbase.resolver;

import com.typesafe.config.Config;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.resolver.AbstractSensitivityTypeResolver;
import org.apache.eagle.security.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HbaseSensitivityTypeResolver extends AbstractSensitivityTypeResolver {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseSensitivityTypeResolver.class);
    private ISecurityMetadataDAO dao;

    public HbaseSensitivityTypeResolver(ApplicationEntityService entityService, Config eagleServerConfig){
        // todo I know this is ugly, but push abstraction to later
        dao = MetadataDaoFactory.getMetadataDAO(eagleServerConfig);
    }

    @Override
    public void init() {
        Map<String, Map<String, String>> maps = getAllSensitivities();
        this.setSensitivityMaps(maps);
    }

    private Map<String, Map<String, String>> getAllSensitivities(){
        Map<String, Map<String, String>> all = new HashMap<>();
        Collection<HBaseSensitivityEntity> entities = dao.listHBaseSensitivies();
        for(HBaseSensitivityEntity entity : entities){
            if(!all.containsKey(entity.getSite())){
                all.put(entity.getSite(), new HashMap<>());
            }
            all.get(entity.getSite()).put(entity.getHbaseResource(), entity.getSensitivityType());
        }
        return all;
    }
}

