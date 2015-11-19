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
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class GenericMetricEntityDecompactionStreamReader extends StreamReader implements EntityCreationListener{
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(GenericMetricEntityDecompactionStreamReader.class);
	private GenericEntityStreamReader reader;
	private EntityDefinition ed;
	private String serviceName = GenericMetricEntity.GENERIC_METRIC_SERVICE;
	private long start;
	private long end;
	private GenericMetricShadowEntity single = new GenericMetricShadowEntity();
	
	/**
	 * it makes sense that serviceName should not be provided while metric name should be provided as prefix
	 * @param metricName
	 * @param condition
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ParseException
	 */
	public GenericMetricEntityDecompactionStreamReader(String metricName, SearchCondition condition) throws InstantiationException, IllegalAccessException, ParseException{
		ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
		checkIsMetric(ed);
		reader = new GenericEntityStreamReader(serviceName, condition, metricName);
		start = DateTimeUtil.humanDateToSeconds(condition.getStartTime())*1000;
		end = DateTimeUtil.humanDateToSeconds(condition.getEndTime())*1000;
	}
	
	private void checkIsMetric(EntityDefinition ed){
		if(ed.getMetricDefinition() == null)
			throw new IllegalArgumentException("Only metric entity comes here");
	}
	
	@Override
	public void entityCreated(TaggedLogAPIEntity entity) throws Exception{
		GenericMetricEntity e = (GenericMetricEntity)entity;
		double[] value = e.getValue();
		if(value != null) {
			int count =value.length;
			@SuppressWarnings("unused")
			Class<?> cls = ed.getMetricDefinition().getSingleTimestampEntityClass();
			for (int i = 0; i < count; i++) {
				long ts = entity.getTimestamp() + i * ed.getMetricDefinition().getInterval();
				// exclude those entity which is not within the time range in search condition. [start, end)
				if (ts < start || ts >= end) {
					continue;
				}
				single.setTimestamp(ts);
				single.setTags(entity.getTags());
				single.setValue(e.getValue()[i]);
				for (EntityCreationListener l : _listeners) {
					l.entityCreated(single);
				}
			}
		}
	}
	
	@Override
	public void readAsStream() throws Exception{
		reader.register(this);
		reader.readAsStream();
	}

	@Override
	public long getLastTimestamp() {
		return reader.getLastTimestamp();
	}

	@Override
	public long getFirstTimestamp() {
		return reader.getFirstTimestamp();
	}
}