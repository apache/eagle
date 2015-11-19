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
import org.apache.eagle.log.entity.meta.EntityConstants;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class GenericEntityScanStreamReader extends StreamReader {
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityScanStreamReader.class);
	
	private EntityDefinition entityDef;
	private SearchCondition condition;
	private String prefix;
	private long lastTimestamp = 0;
	private long firstTimestamp = 0;
	
	public GenericEntityScanStreamReader(String serviceName, SearchCondition condition, String prefix) throws InstantiationException, IllegalAccessException{
		this.prefix = prefix;
		checkNotNull(serviceName, "serviceName");
		this.entityDef = EntityDefinitionManager.getEntityByServiceName(serviceName);
		checkNotNull(entityDef, "EntityDefinition");
		this.condition = condition;
	}

	public GenericEntityScanStreamReader(EntityDefinition entityDef, SearchCondition condition, String prefix) throws InstantiationException, IllegalAccessException{
		this.prefix = prefix;
		checkNotNull(entityDef, "entityDef");
		this.entityDef = entityDef;
		checkNotNull(entityDef, "EntityDefinition");
		this.condition = condition;
	}
	
	public long getLastTimestamp() {
		return lastTimestamp;
	}
	
	private void checkNotNull(Object o, String message){
		if(o == null){
			throw new IllegalArgumentException(message + " should not be null");
		}
	}

	public EntityDefinition getEntityDefinition() {
		return entityDef;
	}

	public SearchCondition getSearchCondition() {
		return condition;
	}

	@Override
	public void readAsStream() throws Exception{
		Date start = null;
		Date end = null;
		// shortcut to avoid read when pageSize=0
		if(condition.getPageSize() <= 0){
			return; // return nothing
		}
		// Process the time range if needed
		if(entityDef.isTimeSeries()){
			start = DateTimeUtil.humanDateToDate(condition.getStartTime());
			end = DateTimeUtil.humanDateToDate(condition.getEndTime());
		}else{
			start = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_START_HUMANTIME);
			end = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_END_HUMANTIME);
		}
		byte[][] outputQualifiers = null;
		if(!condition.isOutputAll()) {
			// Generate the output qualifiers
			outputQualifiers = HBaseInternalLogHelper.getOutputQualifiers(entityDef, condition.getOutputFields());
		}
		HBaseLogReader2 reader = new HBaseLogReader2(entityDef, condition.getPartitionValues(), start, end, condition.getFilter(), condition.getStartRowkey(), outputQualifiers, this.prefix);
		try{
			reader.open();
			InternalLog log;
			int count = 0;
			while ((log = reader.read()) != null) {
				TaggedLogAPIEntity entity = HBaseInternalLogHelper.buildEntity(log, entityDef);
				if (lastTimestamp < entity.getTimestamp()) {
					lastTimestamp = entity.getTimestamp();
				}
				if(firstTimestamp > entity.getTimestamp() || firstTimestamp == 0){
					firstTimestamp = entity.getTimestamp();
				}

				entity.setSerializeVerbose(condition.isOutputVerbose());
				entity.setSerializeAlias(condition.getOutputAlias());

				for(EntityCreationListener l : _listeners){
					l.entityCreated(entity);
				}
				if(++count == condition.getPageSize())
					break;
			}
		}catch(IOException ioe){
			LOG.error("Fail reading log", ioe);
			throw ioe;
		}finally{
			reader.close();
		}		
	}

	@Override
	public long getFirstTimestamp() {
		return this.firstTimestamp;
	}
}
