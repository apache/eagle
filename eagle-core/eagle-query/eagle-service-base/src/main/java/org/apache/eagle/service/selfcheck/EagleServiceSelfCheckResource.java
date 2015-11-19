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
package org.apache.eagle.service.selfcheck;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.eagle.common.config.EagleConfigFactory;

@Path("ValidateInternals")
public class EagleServiceSelfCheckResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public EagleServiceSelfCheckAPIEntity selfCheck(){
		EagleServiceSelfCheckAPIEntity entity = new EagleServiceSelfCheckAPIEntity();
		entity.setHbaseZookeeperQuorum(EagleConfigFactory.load().getZKQuorum());
		entity.setHbaseZookeeperClientPort(EagleConfigFactory.load().getZKPort());
		entity.setEnv(EagleConfigFactory.load().getEnv());
		return entity;
	}
}
