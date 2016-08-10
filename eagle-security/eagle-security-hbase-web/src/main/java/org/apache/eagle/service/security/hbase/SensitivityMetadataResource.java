/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.service.security.hbase;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.security.service.HBaseSensitivityEntity;
import org.apache.eagle.security.service.ISecurityMetadataDAO;
import org.apache.eagle.security.service.MetadataDaoFactory;

import javax.ws.rs.*;
import java.util.Collection;
import java.util.Iterator;

/**
 * Since 6/10/16.
 */
@Path("/metadata/sensitivity")
@Singleton
public class SensitivityMetadataResource {
    private ApplicationEntityService entityService;
    private ISecurityMetadataDAO dao;
    @Inject
    public SensitivityMetadataResource(ApplicationEntityService entityService, Config eagleServerConfig){
        this.entityService = entityService;
        dao = MetadataDaoFactory.getMetadataDAO(eagleServerConfig);
    }

    @Path("/hbase")
    @GET
    @Produces("application/json")
    public Collection<HBaseSensitivityEntity> getHBaseSensitivites(@QueryParam("site") String site){
        return dao.listHBaseSensitivies();
    }

    @Path("/hbase")
    @POST
    @Consumes("application/json")
    public void addHBaseSensitivities(Collection<HBaseSensitivityEntity> list){
        dao.addHBaseSensitivity(list);
    }
}
