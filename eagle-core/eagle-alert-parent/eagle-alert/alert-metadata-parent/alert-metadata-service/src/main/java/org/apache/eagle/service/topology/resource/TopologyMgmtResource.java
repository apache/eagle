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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.topology.resource;

import org.apache.eagle.alert.metadata.resource.OpResult;
import org.apache.eagle.service.topology.resource.impl.TopologyMgmtResourceImpl;
import org.apache.eagle.service.topology.resource.impl.TopologyStatus;

import javax.ws.rs.*;

import java.util.List;

/**
 * @since May 5, 2016
 */
@Path("/alert")
@Produces("application/json")
@Consumes("application/json")
public class TopologyMgmtResource {
    private TopologyMgmtResourceImpl topologyManager = new TopologyMgmtResourceImpl();

    @POST
    @Path("/topologies/{topologyName}/start")
    public OpResult startTopology(@PathParam("topologyName") String topologyName) {
        OpResult result = new OpResult();
        try {
            topologyManager.startTopology(topologyName);
        } catch (Exception ex) {
            result.message = ex.toString();
        }
        return result;
    }

    @POST
    @Path("/topologies/{topologyName}/stop")
    public OpResult stopTopology(@PathParam("topologyName") String topologyName) {
        OpResult result = new OpResult();
        try {
            topologyManager.stopTopology(topologyName);
        } catch (Exception ex) {
            result.message = ex.toString();
        }
        return result;
    }

    @GET
    @Path("/topologies")
    public List<TopologyStatus> getTopologies() throws Exception {
        return topologyManager.getTopologies();
    }

}
