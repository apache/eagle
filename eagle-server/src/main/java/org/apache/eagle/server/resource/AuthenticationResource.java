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
import org.apache.eagle.common.authentication.UserPrincipal;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/authentication")
public class AuthenticationResource {
    @GET
    @Path("/principal")
    @Produces(MediaType.APPLICATION_JSON)
    public UserPrincipal getCurrentPrincipal(@Auth UserPrincipal user) {
        return user;
    }

    @POST
    @Path("/login")
    @Produces(MediaType.APPLICATION_JSON)
    public UserPrincipal login(@Auth UserPrincipal user) {
        return user;
    }
}
