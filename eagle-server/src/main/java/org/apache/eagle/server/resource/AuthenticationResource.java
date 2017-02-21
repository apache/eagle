/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.server.resource;

import io.dropwizard.auth.Auth;
import org.apache.eagle.common.security.User;
import org.apache.eagle.common.rest.RESTResponse;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/auth")
public class AuthenticationResource {
    @GET
    @Path("/principal")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCurrentPrincipal(@Auth(required = false) User user) {
        if (user != null) {
            return RESTResponse.of(user)
                .status(true, Response.Status.OK)
                .build();
        } else {
            return RESTResponse.builder()
                .message("No authorized principal found")
                .status(false, Response.Status.OK)
                .build();
        }
    }

    @GET
    @Path("/validate")
    @Produces(MediaType.APPLICATION_JSON)
    public Response validate(@Auth User user) {
        return RESTResponse.of(user)
            .message("Validated successfully as " + user.getName())
            .status(true, Response.Status.OK).build();
    }

    @POST
    @Path("/login")
    @Produces(MediaType.APPLICATION_JSON)
    public Response login(@Auth User user) {
        return RESTResponse.of(user)
            .message("Login successfully as " + user.getName())
            .status(true, Response.Status.OK).build();
    }
}