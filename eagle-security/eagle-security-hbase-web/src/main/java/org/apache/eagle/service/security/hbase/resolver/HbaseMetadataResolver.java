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

package org.apache.eagle.service.security.hbase.resolver;


import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataAccessConfig;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataAccessConfigDAOImpl;
import org.apache.eagle.service.security.hbase.dao.HbaseMetadataDAOImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HbaseMetadataResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {
    @Override
    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        String query = request.getQuery().trim();
        String site = request.getSite().trim();
        String[] subResources = query.split(":");

        try {
            HbaseMetadataAccessConfig config = new HbaseMetadataAccessConfigDAOImpl().getConfig(site);
            HbaseMetadataDAOImpl dao = new HbaseMetadataDAOImpl(config);

            switch (subResources.length) {
                case 1:
                    if(query.endsWith(":"))
                        return filterAndCombineAttribue(query, dao.getTables(subResources[0]), null);
                    else
                        return filterAndCombineAttribue("", dao.getNamespaces(), subResources[0]);
                case 2:
                    if(query.endsWith(":"))
                        return filterAndCombineAttribue(query, dao.getColumnFamilies(subResources[1]), null);
                    else
                        return filterAndCombineAttribue(subResources[0], dao.getTables(subResources[0]), subResources[1]);
                case 3:
                    return filterAndCombineAttribue(String.format("%s:%s", subResources[0], subResources[1]), dao.getColumnFamilies(subResources[1]), subResources[2]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
        String query = request.getQuery();
        String site = request.getSite();
        if (query == null || query.startsWith(":") || !Pattern.matches("[a-zA-Z:]+", query) || site == null || site.length() == 0) {
            throw new BadAttributeResolveRequestException("hBase resource must be  {\"site\":\"${site}\", \"query\"=\"{namespace}:{table}:{columnFamily}\"");
        }
    }

    public List<String> filterAndCombineAttribue(String prefix, List<String> attrs, String target) {
        List<String> result = new ArrayList<>();
        String format = (prefix == "" ? "%s%s" : "%s:%s");

        if(target == null) {
            for (String attr : attrs){
                result.add(String.format("%s%s", prefix, attr));
            }
        } else {
            Pattern pattern = Pattern.compile("^" + target, Pattern.CASE_INSENSITIVE);
            for (String attr : attrs) {
                if (pattern.matcher(attr).find()) {
                    result.add(String.format(format, prefix, attr));
                }
            }
            if (result.size() == 0) {
                for (String attr : attrs) {
                    result.add(String.format(format, prefix, attr));
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
