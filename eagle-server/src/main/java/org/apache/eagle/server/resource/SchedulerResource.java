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

import com.google.inject.Inject;
import org.apache.eagle.app.environment.impl.ScheduledExecutionRuntime;
import org.quartz.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.List;


@Path("scheduler")
public class SchedulerResource {
    private @Inject ScheduledExecutionRuntime scheduledRuntime;

    @GET
    @Path("/jobs")
    @Produces( {"application/json"})
    public List<JobDetail> getScheduledJobs() throws SchedulerException {
        return scheduledRuntime.getScheduledJobs();
    }

    @GET
    @Path("/triggers")
    @Produces( {"application/json"})
    public List<Trigger> getScheduledTriggers() throws SchedulerException {
        return scheduledRuntime.getScheduledTriggers();
    }

    @GET
    @Path("/")
    @Produces( {"application/json"})
    public SchedulerMetaData getSchedulerMetaData() throws SchedulerException {
        return scheduledRuntime.getScheduler().getMetaData();
    }
}
