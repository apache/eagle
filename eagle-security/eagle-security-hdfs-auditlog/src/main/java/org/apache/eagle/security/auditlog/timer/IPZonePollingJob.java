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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.security.service.IMetadataServiceClient;
import org.apache.eagle.security.service.MetadataServiceClientImpl;
import org.apache.eagle.security.util.ExternalDataCache;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.security.service.IPZoneEntity;
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
			Collection<IPZoneEntity> ipZones = load(jobDataMap);
			if(ipZones == null){
				LOG.warn("Ipzone information is empty");
				return;
			}
			Map<String, IPZoneEntity> map = Maps.uniqueIndex(ipZones, new Function<IPZoneEntity, String>(){
				@Override
				public String apply(IPZoneEntity input) {
					return input.getIphost();
				}
			});
			ExternalDataCache.getInstance().setJobResult(getClass(), map);
		}catch(Exception ex){
			LOG.error("Fail loading ip zones data", ex);
		}
	}

	private Collection<IPZoneEntity> load(JobDataMap jobDataMap) throws Exception{
		String eagleServiceHost = (String)jobDataMap.get(EagleConfigConstants.HOST);
		Integer eagleServicePort = Integer.parseInt(jobDataMap.get(EagleConfigConstants.PORT).toString());
		String username = jobDataMap.containsKey(EagleConfigConstants.USERNAME) ? (String)jobDataMap.get(EagleConfigConstants.USERNAME) : null;
		String password = jobDataMap.containsKey(EagleConfigConstants.PASSWORD) ? (String)jobDataMap.get(EagleConfigConstants.PASSWORD) : null;
		// load from eagle database
		LOG.info("Load ip zone information from eagle service " + eagleServiceHost + ":" + eagleServicePort);
		IMetadataServiceClient client = new MetadataServiceClientImpl(eagleServiceHost, eagleServicePort, "/rest");
		return client.listIPZones();
	}
}
