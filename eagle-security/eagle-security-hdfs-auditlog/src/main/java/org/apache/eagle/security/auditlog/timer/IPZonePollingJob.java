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
package org.apache.eagle.security.auditlog.timer;

import java.util.List;
import java.util.Map;

import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.security.util.ExternalDataCache;
import org.apache.eagle.security.util.ExternalDataCache;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.security.hdfs.entity.IPZoneEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class IPZonePollingJob implements Job{
	private final static Logger LOG = LoggerFactory.getLogger(IPZonePollingJob.class);
	

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		try{
			List<IPZoneEntity> ipZones = load(jobDataMap);
			if(ipZones == null){
				LOG.warn("Ipzone information is empty");
				return;
			}
			Map<String, IPZoneEntity> map = Maps.uniqueIndex(ipZones, new Function<IPZoneEntity, String>(){
				@Override
				public String apply(IPZoneEntity input) {
					return input.getTags().get("iphost");
				}
			});
			ExternalDataCache.getInstance().setJobResult(getClass(), map);
		}catch(Exception ex){
			LOG.error("Fail loading ip zones data", ex);
		}
	}

	private List<IPZoneEntity> load(JobDataMap jobDataMap) throws Exception{
		Map<String, Object> map = (Map<String,Object>)jobDataMap.get(EagleConfigConstants.EAGLE_SERVICE);
		String eagleServiceHost = (String)map.get(EagleConfigConstants.HOST);
		Integer eagleServicePort = (Integer)map.get(EagleConfigConstants.PORT);
		String username = map.containsKey(EagleConfigConstants.USERNAME) ? (String)map.get(EagleConfigConstants.USERNAME) : null;
		String password = map.containsKey(EagleConfigConstants.PASSWORD) ? (String)map.get(EagleConfigConstants.PASSWORD) : null;
		// load from eagle database
		LOG.info("Load ip zone information from eagle service " + eagleServiceHost + ":" + eagleServicePort);
		IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
		String query = "IPZoneService[]{*}";
		GenericServiceAPIResponseEntity<IPZoneEntity> response = client.search()
			.pageSize(Integer.MAX_VALUE)
			.query(query)
			.send();
		client.close();
		if(response.getException() != null)
			throw new IllegalStateException(response.getException());
		return response.getObj();
	}
}
