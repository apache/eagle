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

package org.apache.eagle.service.metadata.resource;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;

import org.codehaus.jackson.type.TypeReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Map;

@Path("/specmetadata")
@Produces("application/json")
@Consumes("application/json")
public class SpecMetadataResource {

    public static final String SPARK = "/spark";

    @Path("/spoutSpec")
    @GET
    public SpoutSpec getSpoutSpec() {
        return MetadataSerDeser.deserialize(getClass().getResourceAsStream(SPARK + "/spoutSpec.json"), SpoutSpec.class);
    }

    @Path("/routerSpec")
    @GET
    public RouterSpec getRouterSpec() {
        return MetadataSerDeser.deserialize(getClass().getResourceAsStream(SPARK + "/streamRouterBoltSpec.json"), RouterSpec.class);
    }

    @Path("/alertBoltSpec")
    @GET
    public AlertBoltSpec getAlertBoltSpec() {
        return MetadataSerDeser.deserialize(getClass().getResourceAsStream(SPARK + "/alertBoltSpec.json"), AlertBoltSpec.class);
    }

    @Path("/publishSpec")
    @GET
    public PublishSpec getPublishSpec() {
        return MetadataSerDeser.deserialize(getClass().getResourceAsStream(SPARK + "/publishSpec.json"), PublishSpec.class);
    }

    @Path("/sds")
    @GET
    public Map<String, StreamDefinition> getSds() {
        return MetadataSerDeser.deserialize(getClass().getResourceAsStream(SPARK + "/streamDefinitionsSpec.json"),
                new TypeReference<Map<String, StreamDefinition>>() {
                });
    }

}
