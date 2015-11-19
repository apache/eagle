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
package org.apache.eagle.query;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @since : 10/30/14,2014
 */
public class GenericEntityQuery implements GenericQuery,EntityCreationListener {
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityQuery.class);

	private List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
	private StreamReader reader;

	public GenericEntityQuery(String serviceName, SearchCondition condition, String metricName) throws IllegalAccessException, InstantiationException {
		if(serviceName.equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
			if(LOG.isDebugEnabled()) LOG.debug("List metric query");
			if(metricName == null || metricName.isEmpty()){
				throw new IllegalArgumentException("metricName should not be empty for metric list query");
			}
			if(!condition.getOutputFields().contains(GenericMetricEntity.VALUE_FIELD)){
				condition.getOutputFields().add(GenericMetricEntity.VALUE_FIELD);
			}
			reader = new GenericEntityStreamReader(serviceName, condition,metricName);
		}else{
			if(LOG.isDebugEnabled()) LOG.debug("List entity query");
			reader = new GenericEntityStreamReader(serviceName, condition);
		}
		reader.register(this);
	}

	@Override
	public long getLastTimestamp() {
		return reader.getLastTimestamp();
	}

	@Override
	public void entityCreated(TaggedLogAPIEntity entity){
		entities.add(entity);
	}

	@Override
	public List<TaggedLogAPIEntity> result() throws Exception{
		if(LOG.isDebugEnabled()) LOG.debug("Start reading as batch mode");
		reader.readAsStream();
		return entities;
	}

	@Override
	public long getFirstTimeStamp() {
		return reader.getFirstTimestamp();
	}
}