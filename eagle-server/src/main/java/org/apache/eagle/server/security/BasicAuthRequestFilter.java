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
package org.apache.eagle.server.security;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.Priority;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.common.security.DenyAll;
import org.apache.eagle.common.security.PermitAll;
import org.apache.eagle.common.security.RolesAllowed;
import org.apache.eagle.common.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import java.lang.reflect.Parameter;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

@Provider
@Priority(Priorities.AUTHORIZATION)
public class BasicAuthRequestFilter implements ContainerRequestFilter {
    private final Authenticator<BasicCredentials, User> authenticator;
    private final AbstractMethod method;
    private static final Logger LOG = LoggerFactory.getLogger(BasicAuthRequestFilter.class);
    private boolean isSecurityDefined = false;
    private boolean isAuthRequired = false;
    private boolean hasPermitAllAnnotation = false;
    private boolean hasDenyAllAnnotation = false;
    private boolean hasRolesAllowedAnnotation = false;

    public BasicAuthRequestFilter(Authenticator<BasicCredentials, User> authenticator, AbstractMethod method) {
        this.authenticator = authenticator;
        this.method = method;
        this.hasPermitAllAnnotation = method.isAnnotationPresent(PermitAll.class);
        this.hasDenyAllAnnotation = method.isAnnotationPresent(DenyAll.class);
        this.hasRolesAllowedAnnotation = method.isAnnotationPresent(RolesAllowed.class);
        this.isSecurityDefined = this.hasPermitAllAnnotation || this.hasDenyAllAnnotation || this.hasRolesAllowedAnnotation;
        for (Parameter parameter : method.getMethod().getParameters()) {
            if (isSecurityDefined && isAuthRequired) {
                break;
            }
            Auth[] authAnnotations = parameter.getAnnotationsByType(Auth.class);
            this.isSecurityDefined = this.isSecurityDefined || authAnnotations.length > 0;
            for (Auth auth : authAnnotations) {
                this.isAuthRequired = this.isAuthRequired || auth.required();
            }
        }
        Preconditions.checkArgument(!(this.hasDenyAllAnnotation && this.hasPermitAllAnnotation), "Conflict @DenyAll and @PermitAll on method " + this.method.toString());
    }


    private static final String AUTHORIZATION_PROPERTY = "Authorization";
    private static final String AUTHENTICATION_SCHEME = "Basic";
    private static final Response UNAUTHORIZED_ACCESS_DENIED = RESTResponse.builder()
        .message("Unauthorized access denied")
        .status(false, Response.Status.UNAUTHORIZED)
        .build();

    private static final Response INVALID_ACCESS_FORBIDDEN = RESTResponse.builder()
        .message("Access denied, invalid username or password")
        .status(false, Response.Status.FORBIDDEN)
        .build();

    private static final Response ALL_ACCESS_FORBIDDEN = RESTResponse.builder()
        .message("Access denied")
        .status(false, Response.Status.FORBIDDEN)
        .build();

    @Override
    public ContainerRequest filter(ContainerRequest containerRequest) {
        try {
            if (!isSecurityDefined) {
                return containerRequest;
            }
            //Access denied for all

            if (hasDenyAllAnnotation) {
                throw new WebApplicationException(ALL_ACCESS_FORBIDDEN);
            }

            //Get request headers
            final MultivaluedMap<String, String> headers = containerRequest.getRequestHeaders();

            //Fetch authorization header
            final List<String> authorization = headers.get(AUTHORIZATION_PROPERTY);

            //If no authorization information present; block access
            if ((authorization == null || authorization.isEmpty()) && isAuthRequired) {
                throw new WebApplicationException(UNAUTHORIZED_ACCESS_DENIED);
            }

            if (authorization != null) {
                //Get encoded username and password
                final String encodedUserPassword = authorization.get(0).replaceFirst(AUTHENTICATION_SCHEME + " ", "");

                //Decode username and password
                String usernameAndPassword = new String(Base64.decode(encodedUserPassword.getBytes()));

                //Split username and password tokens
                final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
                final String username = tokenizer.hasMoreElements() ? tokenizer.nextToken() : null;
                final String password = tokenizer.hasMoreElements() ? tokenizer.nextToken() : null;

                if (username == null || password == null) {
                    if (this.isSecurityDefined) {
                        throw new WebApplicationException(RESTResponse.builder()
                            .status(false, Response.Status.FORBIDDEN)
                            .message("Access forbidden, invalid username or password")
                            .build());
                    } else {
                        return containerRequest;
                    }
                }

                Optional<User> userOptional = this.authenticator.authenticate(new BasicCredentials(username, password));
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    containerRequest.setSecurityContext(new SecurityContext() {
                        @Override
                        public Principal getUserPrincipal() {
                            return user;
                        }

                        @Override
                        public boolean isUserInRole(String role) {
                            return user.isInRole(User.Role.locateCaseInsensitive(role));
                        }

                        @Override
                        public boolean isSecure() {
                            return "https".equals(containerRequest.getRequestUri().getScheme());
                        }

                        @Override
                        public String getAuthenticationScheme() {
                            return "Basic Auth";
                        }
                    });

                    //Verify user access
                    if (hasRolesAllowedAnnotation && !hasPermitAllAnnotation) {
                        RolesAllowed rolesAnnotation = method.getAnnotation(RolesAllowed.class);
                        if (!user.isInRole(rolesAnnotation.value())) {
                            throw new WebApplicationException(
                                RESTResponse.builder()
                                    .status(false, Response.Status.FORBIDDEN)
                                    .message("Access forbidden, required roles: " + Arrays.toString(rolesAnnotation.value()))
                                    .build());
                        }
                    }
                } else {
                    throw new WebApplicationException(INVALID_ACCESS_FORBIDDEN);
                }
            } else {
                throw new WebApplicationException(UNAUTHORIZED_ACCESS_DENIED);
            }
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Server authentication error: " + e.getMessage(), e);
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("Server authentication error: " + e.getMessage()).build());
        }
        return containerRequest;
    }
}