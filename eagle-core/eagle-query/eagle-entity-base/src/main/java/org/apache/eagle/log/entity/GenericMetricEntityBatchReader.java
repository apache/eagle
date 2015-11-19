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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GenericMetricEntityBatchReader  implements EntityCreationListener{
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityBatchReader.class);
	
	private List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
	private GenericEntityStreamReader reader;
	
	public GenericMetricEntityBatchReader(String metricName, SearchCondition condition) throws Exception{
		reader = new GenericEntityStreamReader(GenericMetricEntity.GENERIC_METRIC_SERVICE, condition, metricName);
	}
	
	public long getLastTimestamp() {
		return reader.getLastTimestamp();
	}
	public long getFirstTimestamp() {
		return reader.getFirstTimestamp();
	}
	@Override
	public void entityCreated(TaggedLogAPIEntity entity){
		entities.add(entity);
	}
	
	@SuppressWarnings("unchecked")
	public <T> List<T> read() throws Exception{
		if(LOG.isDebugEnabled()) LOG.debug("Start reading as batch mode");
		reader.register(this);
		reader.readAsStream();
		return (List<T>)entities;
	}
}
