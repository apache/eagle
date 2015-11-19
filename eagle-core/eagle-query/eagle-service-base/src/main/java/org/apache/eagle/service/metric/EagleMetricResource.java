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
package org.apache.eagle.service.metric;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.GenericCreateAPIResponseEntity;
import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.GenericMetricEntity;

@Path(EagleMetricResource.METRIC_URL_PATH)
public class EagleMetricResource {
	private static final Logger LOG = LoggerFactory.getLogger(EagleMetricResource.class);
	public static final String METRIC_URL_PATH = "/metric";
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public GenericCreateAPIResponseEntity createGenericMetricEntity(List<GenericMetricEntity> entities) {
		GenericCreateAPIResponseEntity result = new GenericCreateAPIResponseEntity();
		try{
			GenericEntityWriter writer = new GenericEntityWriter(GenericMetricEntity.GENERIC_METRIC_SERVICE);
			List<String> rowkeys = null;
			rowkeys = writer.write(entities);
			result.setEncodedRowkeys(rowkeys);
			result.setSuccess(true);
			return result;
		}catch(Exception ex){
			LOG.error("Fail writing Generic Metric entity", ex);
			result.setSuccess(false);
			result.setException(ex.getMessage());
			return result;
		}
	}
}
