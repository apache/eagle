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
package org.apache.eagle.alert.coordinator.resource;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.Coordinator;
import org.apache.eagle.alert.coordinator.ScheduleOption;
import org.apache.eagle.alert.utils.JsonUtils;

/**
 * This is to provide API access even we don't have ZK as intermediate access.
 * FIXME : more elogant status code
 * 
 * @since Mar 24, 2016 <br/>
 */
@Path("/coordinator")
@Produces({ "application/json" })
public class CoordinatorResource {

    // sprint config here?
    private Coordinator alertCoordinator = new Coordinator();

    @GET
    @Path("/assignments")
    public String getAssignments() throws Exception {
        ScheduleState state = alertCoordinator.getState();
        return JsonUtils.writeValueAsString(state);
    }

    @POST
    @Path("/build")
    public String build() throws Exception {
        ScheduleOption option = new ScheduleOption();
        ScheduleState state = alertCoordinator.schedule(option);
        return JsonUtils.writeValueAsString(state);
    }

    @POST
    @Path("/enablePeriodicForceBuild")
    public void enforcePeriodicallyBuild() {
        alertCoordinator.enforcePeriodicallyBuild();
    }

    @POST
    @Path("/disablePeriodicForceBuild")
    public void disablePeriodicallyBuild() {
        alertCoordinator.disablePeriodicallyBuild();
    }
    
    @SuppressWarnings("static-access")
    @GET
    @Path("/periodicForceBuildState")
    public boolean statPeriodicallyBuild() {
        return alertCoordinator.isPeriodicallyForceBuildEnable();
    }

    /**
     * Manually update the topology usages, for administration
     * 
     * @return
     */
    @POST
    @Path("/refreshUsages")
    public String refreshUsages() {
        // TODO
        return "";
    }

}
