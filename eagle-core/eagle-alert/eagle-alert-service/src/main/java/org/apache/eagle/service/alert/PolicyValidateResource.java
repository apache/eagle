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
package org.apache.eagle.service.alert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import com.fasterxml.jackson.databind.Module;

@Path("/policy/validate")
public class PolicyValidateResource {
		
	public static Logger LOG = LoggerFactory.getLogger(PolicyValidateResource.class);
	
	@SuppressWarnings({"rawtypes"})
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public GenericServiceAPIResponseEntity validatePolicy(String policyToValidate) {
        ServiceLoader<AlertPolicyValidateProvider> loader = ServiceLoader.load(AlertPolicyValidateProvider.class);
        Iterator<AlertPolicyValidateProvider> iter = loader.iterator();
        List<Module> modules = new ArrayList<Module>();
        while(iter.hasNext()) {
        	AlertPolicyValidateProvider factory = iter.next();
            LOG.info("Supported policy type : " + factory.PolicyType());
            modules.addAll(factory.BindingModules());
        }
        AlertPolicyValidateProvider policyValidate = null;
        try {
        	policyValidate = JsonSerDeserUtils.deserialize(policyToValidate, AlertPolicyValidateProvider.class, modules);    		
        }
        catch (Exception ex) {
        	LOG.error("Fail consutructing AlertPolicyValidateProvider ", ex);
        }
        return policyValidate.validate();
	}
}