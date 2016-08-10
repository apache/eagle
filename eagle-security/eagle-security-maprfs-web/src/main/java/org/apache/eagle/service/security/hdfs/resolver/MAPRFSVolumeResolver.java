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
package org.apache.eagle.service.security.hdfs.resolver;

import com.typesafe.config.Config;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.eagle.service.security.hdfs.MAPRFSResourceConstants;
import org.apache.eagle.service.security.hdfs.rest.HttpRequest;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MAPRFSVolumeResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {

    private final static Logger LOG = LoggerFactory.getLogger(MAPRFSVolumeResolver.class);

    @Override
    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        String query = request.getQuery().trim();
        List<String> volumelist = new ArrayList<>();
        MetadataAccessConfigRepo repo = new MetadataAccessConfigRepo();
        //Call MAPR REST API to get volumes
        try {
            Config config = repo.getConfig(MAPRFSResourceConstants.MAPRFS_APPLICATION, request.getSite().trim());
            Configuration conf = repo.convert(config);
            //get user name, password, make request to mapr rest service
            String username;
            String password;
            username = config.getString(MAPRFSResourceConstants.MAPRFS_USERNAME);
            password = config.getString(MAPRFSResourceConstants.MAPRFS_PASSWORD);
            //constuct url to query mapr volume
            String restUrl = config.getString(MAPRFSResourceConstants.MAPRFS_WEBUI_HTTPS) + MAPRFSResourceConstants.MAPRFS_LIST_VOLUME;

            JSONObject response = HttpRequest.executeGet(restUrl,username,password);
            volumelist = extractVolumeList(response);
            List<String> res = new ArrayList<>();
            for(String status : volumelist) {
                Pattern pattern = Pattern.compile("^" + query, Pattern.CASE_INSENSITIVE);
                if(pattern.matcher(status).find()) {
                    res.add(status);
                }
            }
            if(res.size() == 0) {
                return volumelist;
            }
            return res;
        } catch( Exception e ) {
            LOG.error(" Exception in MAPRFS Volume Resolver ", e);
            throw new AttributeResolveException(e);
        }
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
        ;
    }

    @Override
    public Class<GenericAttributeResolveRequest> getRequestClass() {
        return GenericAttributeResolveRequest.class;
    }


    public List<String> extractVolumeList(JSONObject response){
        // rest url:  https://sandbox.map.com:8443/rest/volume/list
        List<String> result = new ArrayList<>();
        JSONArray list = (JSONArray) response.get("data");
        for(int i = 0; i< list.length();i++ ){
            JSONObject element = (JSONObject) list.get(i);
            result.add(element.getString("volumename"));
        }
        return result;
    }

}