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
package org.apache.eagle.service.security.oozie.res;

import org.apache.eagle.security.entity.OozieResourceEntity;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.service.security.oozie.OozieResourceSensitivityDataJoiner;
import org.apache.eagle.service.security.oozie.dao.*;
import org.apache.oozie.client.CoordinatorJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/oozieResource")
public class OozieMetadataBrowseWebResource {

    private static Logger LOG = LoggerFactory.getLogger(OozieMetadataBrowseWebResource.class);

    @Path("/coordinators")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public OozieMetadataBrowseWebResponse getDatabases(@QueryParam("site") String site){

        OozieMetadataBrowseWebResponse response = new OozieMetadataBrowseWebResponse();
        List<CoordinatorJob> coordinators = null;
        try {
            OozieMetadataAccessConfig config = new OozieMetadataAccessConfigDAOImpl().getConfig(site);
            OozieMetadataDAO dao = new OozieMetadataDAOImpl(config);
            coordinators = dao.getCoordJobs();
        } catch(Exception ex){
            LOG.error("fail getting coordinators", ex);
            response.setException(EagleExceptionWrapper.wrap(ex));
        }
        List<OozieResourceEntity> result = new ArrayList<>();
        OozieResourceSensitivityDataJoiner joiner = new OozieResourceSensitivityDataJoiner();
        result = joiner.joinOozieResourceSensitivity(site, coordinators);
        response.setObj(result);
        return response;
    }
}
