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
package org.apache.eagle.app.proxy.stream;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.eagle.app.messaging.StreamRecord;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.common.security.RolesAllowed;
import org.apache.eagle.common.security.User;
import org.apache.eagle.metadata.model.StreamDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Path("/streams")
public class StreamProxyResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamProxyResource.class);
    @Inject
    private StreamProxyManager proxyManager;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Collection<StreamDesc>> getAllStreamDesc() {
        return RESTResponse.async(() -> proxyManager.getAllStreamDesc()).get();
    }

    @POST
    @Path("/{streamId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @RolesAllowed( {User.Role.ADMINISTRATOR, User.Role.APPLICATION})
    public RESTResponse produceEvent(@NotNull List<StreamRecord> records, @PathParam("streamId") String streamId) {
        return RESTResponse.async((builder) -> {
            try {
                Preconditions.checkNotNull(records, "Records is empty");
                proxyManager.getStreamProxy(streamId).send(records);
                builder.status(true, Response.Status.OK)
                    .message(String.format("Successfully wrote %s records into stream %s", records.size(), streamId));
            } catch (Exception e) {
                LOGGER.error("Error to write records to stream {}: {}", streamId, e.getMessage(), e);
                builder.exception(e)
                    .status(false, Response.Status.BAD_REQUEST)
                    .message("Failed to write messages to stream " + streamId + ": " + e.getMessage());
            }
        }).get();
    }

    @GET
    @Path("/{streamId}")
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse getSingleStreamDesc(@PathParam("streamId") String streamId) {
        return RESTResponse.async((builder) -> {
            Optional<StreamDesc> streamDesc = proxyManager.getAllStreamDesc()
                .stream().filter((desc) -> desc.getStreamId().equalsIgnoreCase(streamId)).findAny();
            if (streamDesc.isPresent()) {
                builder.data(streamDesc.get())
                    .status(true, Response.Status.OK);
            } else {
                builder.message("Stream not found, reason: stream not exist or proxy not initialized").status(false, Response.Status.BAD_REQUEST);
            }
        }).get();
    }
}