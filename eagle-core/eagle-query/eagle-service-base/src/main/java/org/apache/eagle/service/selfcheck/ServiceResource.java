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

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import com.sun.jersey.api.json.JSONWithPadding;

@Path("services")
public class ServiceResource {
	private static final Logger LOG = LoggerFactory.getLogger(ServiceResource.class);
	
	@GET
	@Produces({"application/json", "application/xml"})
	public Map<String, EntityDefinition> getServiceDefinitions(){
		Map<String, EntityDefinition> result = null;
		try{
			 result = EntityDefinitionManager.entities();
		}catch(Exception ex){
			LOG.error("Error in getting entity definitions", ex);
		}
		
		return result;
	}
	
	@GET
	@Path("/jsonp")
	@Produces({"application/x-javascript", "application/json", "application/xml"})
	public JSONWithPadding getServiceDefinitionsWithJsonp(@QueryParam("callback") String callback){
		Map<String, EntityDefinition> result = null;
		try{
			 result = EntityDefinitionManager.entities();
		}catch(Exception ex){
			LOG.error("Error in getting entity definitions", ex);
		}
		
		return new JSONWithPadding(new GenericEntity<Map<String, EntityDefinition>>(result){}, callback);
	}
}
