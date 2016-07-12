/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.resource;


import com.google.inject.Inject;
import org.apache.eagle.metadata.model.ApplicationSpec;
import org.apache.eagle.metadata.service.ApplicationSpecService;

import javax.annotation.Resource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.List;

@Resource
public class ApplicationResource {
    private final ApplicationSpecService applicationSpecService;

    @Inject
    public ApplicationResource(ApplicationSpecService applicationSpecService){
        this.applicationSpecService = applicationSpecService;
    }

    @Path("/v1/apps")
    @GET
    public List<ApplicationSpec> getAvailableApplications(){
        return applicationSpecService.getAllApplicationSpecs();
    }

    @Path("/v1/apps/{appType}")
    @GET
    public ApplicationSpec getApplication(@PathParam("appType") String appType){
        return applicationSpecService.getApplicationSpecByType(appType);
    }
}