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

package org.apache.eagle.service.alert.resolver;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/site")
public class SiteApplicationResource {
    private final static Logger LOG = LoggerFactory.getLogger(SiteApplicationResource.class);
    private final static GenericEntityServiceResource resource = new GenericEntityServiceResource();

    @Path("/delete")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteSite(@QueryParam("site") String site) {
        String siteQuery = Constants.SITE_DESCRIPTION_SERVICE_ENDPOINT_NAME+ "[@site=\"" + site + "\"]{*}";
        String siteApplicationQuery = Constants.SITE_APPLICATION_SERVICE_ENDPOINT_NAME + "[@site=\"" + site + "\"]{*}";
        int pageSize = Integer.MAX_VALUE;

        GenericServiceAPIResponseEntity response = resource.deleteByQuery(siteQuery, null, null, pageSize, null, false, false, 0L, 0, true, 0, null, false);
        if(response.isSuccess()) {
            response = resource.deleteByQuery(siteApplicationQuery, null, null, pageSize, null, false, false, 0L, 0, true, 0, null, false);
            if(!response.isSuccess()) {
                LOG.error(response.getException());
            }
        } else {
            LOG.error(response.getException());
        }
        return response;
    }

}
