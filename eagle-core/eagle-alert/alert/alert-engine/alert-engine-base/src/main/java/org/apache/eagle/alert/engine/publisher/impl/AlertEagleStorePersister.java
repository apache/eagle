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

package org.apache.eagle.alert.engine.publisher.impl;

import java.util.List;

import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * Alert API entity Persistor
 */
public class AlertEagleStorePersister {
	private static Logger LOG = LoggerFactory.getLogger(AlertEagleStorePersister.class);
	private Config config;

	public AlertEagleStorePersister(Config config) {
		this.config = config;
	}

	/**
	 * Persist passes list of Entities
	 * @param list
	 * @return
     */
	public boolean doPersist(List<? extends StreamEvent> list) {
		if (list.isEmpty()) return false;
		LOG.info("Going to persist entities, type: " + " " + list.get(0).getClass().getSimpleName() + ", list size: " + list.size());
		try {
			IMetadataServiceClient client = new MetadataServiceClientImpl(config);
			// TODO: metadata service support
		}
		catch (Exception ex) {
			LOG.error("Got an exception in persisting entities", ex);
			return false;
		}
		return false;
	}
}
