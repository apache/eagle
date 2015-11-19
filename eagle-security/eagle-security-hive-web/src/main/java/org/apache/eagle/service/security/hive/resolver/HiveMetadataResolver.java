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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.eagle.service.security.hive.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveMetadataResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {
	private final static Logger LOG = LoggerFactory.getLogger(HiveMetadataResolver.class);
    private final static String HIVE_ATTRIBUTE_RESOLVE_FORMAT_HINT =
            "hive metadata resolve must be {\"site\":\"${site}\", \"query\"=\"/{db}/{table}/{column}\"}";
    private HiveSensitivityMetadataDAOImpl dao = new HiveSensitivityMetadataDAOImpl();
    private Map<String, Map<String, String>> maps = dao.getAllHiveSensitivityMap();

    @Override
    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        // query should be formatted as "/{db}/{table}/{column}" with "/" as leading character
        String query = request.getQuery().trim();
        String[] subResources = query.split("/");
        String prefix = null;

        try {
            HiveMetadataAccessConfig config = new HiveMetadataAccessConfigDAOImpl().getConfig(request.getSite());
            HiveMetadataDAO dao = new HiveMetadataDAOFactory().getHiveMetadataDAO(config);
            if (subResources.length == 0) { // query all databases with "/"
                return filterAndCombineAttribue("/", dao.getDatabases(), null);
            }else if(subResources.length == 2){ // query all tables given a database
                if(query.endsWith("/")) {
                    prefix = String.format("/%s/", subResources[1]);
                    return filterAndCombineAttribue(prefix, dao.getTables(subResources[1]), null);
                }
                return filterAndCombineAttribue("/", dao.getDatabases(), subResources[1]);
            }else if(subResources.length == 3){
                if(query.endsWith("/")) {
                    prefix = String.format("/%s/%s/", subResources[1], subResources[2]);
                    return filterAndCombineAttribue(prefix, dao.getColumns(subResources[1], subResources[2]), null);
                } else {
                    prefix = String.format("/%s/", subResources[1]);
                    return filterAndCombineAttribue(prefix, dao.getTables(subResources[1]), subResources[2]);
                }
            }else if(subResources.length == 4) {
                prefix = String.format("/%s/%s/", subResources[1], subResources[2]);
                return filterAndCombineAttribue(prefix, dao.getColumns(subResources[1], subResources[2]), subResources[3]);
            }

        }catch(Exception ex){
            LOG.error("error fetching hive metadata", ex);
            throw new AttributeResolveException(ex);
        }
        return Arrays.asList(request.getQuery());
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
    	String query = request.getQuery();
        String site = request.getSite();
        if (query == null || !query.startsWith("/") || query.split("/").length > 4 || site == null || site.length() == 0) {
            throw new BadAttributeResolveRequestException(HIVE_ATTRIBUTE_RESOLVE_FORMAT_HINT);
        }
    }

    public List<String> filterAndCombineAttribue(String prefix, List<String> attrs, String target) {
        List<String> result = new ArrayList<>();
        if(target == null) {
            for (String attr : attrs){
                result.add(String.format("%s%s", prefix, attr));
            }
        } else {
            Pattern pattern = Pattern.compile("^" + target, Pattern.CASE_INSENSITIVE);
            for (String attr : attrs) {
                if (pattern.matcher(attr).find()){
                    result.add(String.format("%s%s", prefix, attr));
                }
            }
            if(result.size() == 0) {
                for (String attr : attrs) {
                    result.add(String.format("%s%s", prefix, attr));
                }
            }
        }
        return result;
    }

    @Override
    public Class<GenericAttributeResolveRequest> getRequestClass() {
        return GenericAttributeResolveRequest.class;
    }
}
