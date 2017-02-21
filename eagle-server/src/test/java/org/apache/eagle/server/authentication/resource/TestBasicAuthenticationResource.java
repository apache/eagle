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
 */
package org.apache.eagle.server.authentication.resource;

import io.dropwizard.auth.Auth;
import org.apache.eagle.common.security.DenyAll;
import org.apache.eagle.common.security.PermitAll;
import org.apache.eagle.common.security.RolesAllowed;
import org.apache.eagle.common.security.User;
import org.junit.Ignore;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

@Ignore
@Path("/testAuth")
public class TestBasicAuthenticationResource {
    @GET
    @Path("/userOnly")
    @Produces(MediaType.APPLICATION_JSON)
    public User getUser(@Auth User user) {
        return user;
    }

    @GET
    @Path("/adminOnly")
    @Produces(MediaType.APPLICATION_JSON)
    @RolesAllowed({User.Role.ADMINISTRATOR})
    public User getAdminUser(@Auth User user) {
        return user;
    }

    @GET
    @Path("/userOrAdmin")
    @Produces(MediaType.APPLICATION_JSON)
    @RolesAllowed({User.Role.ADMINISTRATOR, User.Role.USER})
    public User getUserOrAdmin(@Auth User user) {
        return user;
    }

    @GET
    @Path("/securityContext")
    @Produces(MediaType.APPLICATION_JSON)
    public SecurityContext getSecurityContext(@Context SecurityContext securityContext) {
        return securityContext;
    }

    @GET
    @Path("/permitAll")
    @Produces(MediaType.APPLICATION_JSON)
    @PermitAll
    public User getPermitAllUser(@Auth(required = false) User user) {
        return user;
    }

    @GET
    @Path("/denyAll")
    @Produces(MediaType.APPLICATION_JSON)
    @DenyAll
    public User getDenyAllUser(@Auth User user) {
        return user;
    }
}