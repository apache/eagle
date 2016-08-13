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
package org.apache.eagle.service.security.hive.resolver;

import com.typesafe.config.Config;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.service.HiveSensitivityEntity;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.apache.eagle.security.service.MetadataDaoFactory;
import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class HiveSensitivityTypeResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {
    private final static Logger LOG = LoggerFactory.getLogger(HiveSensitivityTypeResolver.class);
    private ISecurityMetadataDAO dao;
    private ApplicationEntityService entityService;

    public HiveSensitivityTypeResolver(ApplicationEntityService entityService, Config eagleServerConfig){
        this.entityService = entityService;
        dao = MetadataDaoFactory.getMetadataDAO(eagleServerConfig);
    }
    private final static String SENSITIVETYPE_ATTRIBUTE_RESOLVE_FORMAT_HINT = "Sensitive type should be composed of a-z, A-Z, 0-9 or -";

    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        String query = request.getQuery().trim();
        String site = request.getSite().trim();
        List<String> res = new ArrayList<>();
        Map<String, String> map = getAllSensitivities().get(site);

        if(map == null) {
            return res;
        }
        List<String> sensitiveTypes = new ArrayList<>(map.values());

        for(String type : sensitiveTypes) {
            Pattern pattern = Pattern.compile("^" + query, Pattern.CASE_INSENSITIVE);
            if(pattern.matcher(type).find()) {
                res.add(type);
            }
        }
        if(res.size() == 0) {
            return sensitiveTypes;
        }
        return res;
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
        String query = request.getQuery();
        if (query == null || !Pattern.matches("\\w+", query)) {
            throw new BadAttributeResolveRequestException(SENSITIVETYPE_ATTRIBUTE_RESOLVE_FORMAT_HINT);
        }
    }

    @Override
    public Class<GenericAttributeResolveRequest> getRequestClass() {
        return GenericAttributeResolveRequest.class;
    }

    private Map<String, Map<String, String>> getAllSensitivities(){
        Map<String, Map<String, String>> all = new HashMap<>();
        Collection<HiveSensitivityEntity> entities = dao.listHiveSensitivities();
        for(HiveSensitivityEntity entity : entities){
            if(!all.containsKey(entity.getSite())){
                all.put(entity.getSite(), new HashMap<>());
            }
            all.get(entity.getSite()).put(entity.getHiveResource(), entity.getSensitivityType());
        }
        return all;
    }
}
