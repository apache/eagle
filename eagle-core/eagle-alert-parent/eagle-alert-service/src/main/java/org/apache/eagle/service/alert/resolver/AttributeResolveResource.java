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
package org.apache.eagle.service.alert.resolver;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import java.io.InputStream;
import java.util.List;
import javax.ws.rs.*;

/**
 * @since 6/17/15.
 */
@Path("/stream")
public class AttributeResolveResource {
    private ApplicationEntityService entityService;
    private Config eagleServerConfig;

    @Inject
    public AttributeResolveResource(ApplicationEntityService entityService, Config eagleServerConfig) {
        this.entityService = entityService;
        this.eagleServerConfig = eagleServerConfig;
    }

    @POST
    @Path("attributeresolve")
    @Consumes( {"application/json"})
    @Produces( {"application/json"})
    public GenericServiceAPIResponseEntity attributeResolve(InputStream request,
                                                            @QueryParam("resolver") String resolver) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            if (resolver == null) {
                throw new AttributeResolveException("resolver is null");
            }
            AttributeResolvable resolvable = AttributeResolverFactory.getAttributeResolver(resolver, entityService, eagleServerConfig);
            ObjectMapper objectMapper = new ObjectMapper();
            Class<?> resolveRequestClass = resolvable.getRequestClass();
            if (resolveRequestClass == null) {
                throw new AttributeResolveException("Request class is null for resolver " + resolver);
            }
            GenericAttributeResolveRequest resolveRequest = (GenericAttributeResolveRequest) objectMapper.readValue(request, resolvable.getRequestClass());
            resolvable.validateRequest(resolveRequest);
            List result = resolvable.resolve(resolveRequest);
            response.setSuccess(true);
            response.setObj(result);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setException(e);
            return response;
        }
        return response;
    }

    @GET
    @Path("attributeresolve")
    @Produces( {"application/json"})
    public GenericServiceAPIResponseEntity attributeResolver(
        @QueryParam("resolver") String resolver, @QueryParam("site") String site, @QueryParam("query") String query) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            if (resolver == null) {
                throw new AttributeResolveException("resolver is null");
            }
            AttributeResolvable resolvable = AttributeResolverFactory.getAttributeResolver(resolver, entityService, eagleServerConfig);
            Class<?> resolveRequestClass = resolvable.getRequestClass();
            if (resolveRequestClass == null) {
                throw new AttributeResolveException("Request class is null for resolver " + resolver);
            }
            GenericAttributeResolveRequest resolveRequest = new GenericAttributeResolveRequest(query, site);
            resolvable.validateRequest(resolveRequest);
            List result = resolvable.resolve(resolveRequest);
            response.setSuccess(true);
            response.setObj(result);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setException(e);
            return response;
        }
        return response;
    }
}
