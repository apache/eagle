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
package org.apache.eagle.security.service;

import javax.ws.rs.*;
import java.util.Collection;

/**
 * Since 6/10/16.
 */
@Path("/metadata/sensitivity")
public class SensitivityMetadataResource {
    public SensitivityMetadataResource(){
    }

    @Path("/hbase")
    @GET
    @Produces("application/json")
    public Collection<HBaseSensitivityEntity> getHBaseSensitivites(){
        ISecurityMetadataDAO dao = MetadataDaoFactory.getInstance().getMetadataDao();
        return dao.listHBaseSensitivies();
    }

    @Path("/hbase")
    @POST
    @Consumes("application/json")
    public void addHBaseSensitivities(Collection<HBaseSensitivityEntity> list){
        ISecurityMetadataDAO dao = MetadataDaoFactory.getInstance().getMetadataDao();
        dao.addHBaseSensitivity(list);
    }
}
