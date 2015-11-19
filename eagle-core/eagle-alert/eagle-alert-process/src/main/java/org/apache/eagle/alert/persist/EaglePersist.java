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
/**
 * 
 */
package org.apache.eagle.alert.persist;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EaglePersist {
		
	private static Logger LOG = LoggerFactory.getLogger(EaglePersist.class);
	private String eagleServiceHost;
	private int eagleServicePort;
	private String username;
	private String password;

	public EaglePersist(String eagleServiceHost, int eagleServicePort) {
		this(eagleServiceHost, eagleServicePort, null, null);
	}

	public EaglePersist(String eagleServiceHost, int eagleServicePort, String username, String password) {
		this.eagleServiceHost = eagleServiceHost;
		this.eagleServicePort = eagleServicePort;
		this.username = username;
		this.password = password;
	}
	
	public boolean doPersist(List<? extends TaggedLogAPIEntity> list) {
		if (list.isEmpty()) return false;
		LOG.info("Going to persist entities, type: " + " " + list.get(0).getClass().getSimpleName() + ", list size: " + list.size());
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
			GenericServiceAPIResponseEntity<String> response = client.create(list);
			client.close();
			if (response.isSuccess()) {
				LOG.info("Successfully create entities " + list.toString());
				return true;
			}
			else {
				LOG.error("Fail to create entities");
				return false;
			}
		}
		catch (Exception ex) {
			LOG.error("Got an exception in persisting entities" + ex.getMessage(), ex);
			return false;
		}
	}
}
