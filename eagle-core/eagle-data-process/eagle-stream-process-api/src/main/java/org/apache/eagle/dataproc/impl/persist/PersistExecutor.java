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

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.apache.eagle.dataproc.impl.persist.druid.DruidPersistService;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.datastream.core.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.List;

/**
 *
 * TODO: currently only accept to be used after aggregation node (See the AggregateEntity reference here).
 * @since Dec 19, 2015
 *
 */
public class PersistExecutor extends JavaStormStreamExecutor2<String, AggregateEntity> {
	
	private static final Logger LOG = LoggerFactory.getLogger(PersistExecutor.class);

	private Config config;
	private IPersistService<AggregateEntity> persistService;
	private String persistExecutorId;
	private String persistType;

	public PersistExecutor(String persistExecutorId, String persistType) {
		this.persistExecutorId = persistExecutorId;
		this.persistType = persistType;
	}

    @Override
	public void prepareConfig(Config config) {
		this.config = config;
	}

    @Override
	public void init() {
		if (persistType.equalsIgnoreCase(StorageType.DRUID().toString())) {
			Config subConfig = this.config.atKey("persistExecutorConfigs." + persistExecutorId);
			persistService = new DruidPersistService(subConfig);
		} else {
			throw new RuntimeException(String.format("Persist type '%s' not supported yet!", persistService));
		}
	}

	@Override
	public void flatMap(List<Object> input, Collector<Tuple2<String, AggregateEntity>> collector) {
		if (input.size() != 2) {
			LOG.error(String.format("Persist executor expect two elements per tuple. But actually got size %d lists!",
					input.size()));
			return;
		}

		String policyId = (String) input.get(0);
		AggregateEntity entity = (AggregateEntity) input.get(1);
		try {
			persistService.save("defaultOutput", entity);
		} catch (Exception e) {
			LOG.error(MessageFormat.format("persist entity failed: {0}", entity), e);
		}
	}

}
