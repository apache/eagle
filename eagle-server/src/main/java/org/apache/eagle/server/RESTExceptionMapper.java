/**
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
package org.apache.eagle.server;

import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.eagle.metadata.resource.RESTResponse;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ThreadLocalRandom;

public class RESTExceptionMapper extends LoggingExceptionMapper<Throwable> {
    @Override
    public Response toResponse(Throwable throwable) {
        final long id = ThreadLocalRandom.current().nextLong();
        logException(id, throwable);
        if (throwable instanceof WebApplicationException) {
            return ((WebApplicationException) throwable).getResponse();
        }
       return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new RESTResponse<>(throwable)).type(MediaType.APPLICATION_JSON_TYPE).build();
    }
}