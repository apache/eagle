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
package org.apache.eagle.dataproc.impl.persist;

import java.util.List;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * @since Dec 19, 2015
 *
 */
public class PersistExecutor extends JavaStormStreamExecutor2<String, AlertAPIEntity> {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(PersistExecutor.class);

	private Config config;
	private IPersistService<AlertAPIEntity> persistService;
	private String executorId;

	public PersistExecutor() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepareConfig(Config config) {
		this.config = config;
		
		String persistType = this.config.getString("persistConfigs" + "." + "type");
		
	}

	@Override
	public void init() {
	}

	@Override
	public void flatMap(List input, Collector collector) {
		// TODO Auto-generated method stub
		if (input.size() != 2) {
			LOG.error(String.format("Persist executor expect two elements per tuple. But actually got size %d lists",
					input.size()));
			return;
		}
		
		String streamId = (String)input.get(0);
		AlertAPIEntity entity = (AlertAPIEntity)input.get(1);
	}

	
}
