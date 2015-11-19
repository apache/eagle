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
import org.apache.eagle.common.EagleBase64Wrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GenericEntityWriter {
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityWriter.class);
	private EntityDefinition entityDef;

	public GenericEntityWriter(String serviceName) throws InstantiationException, IllegalAccessException{
		this.entityDef = EntityDefinitionManager.getEntityByServiceName(serviceName);
		checkNotNull(entityDef, "serviceName");
	}

	public GenericEntityWriter(EntityDefinition entityDef) throws InstantiationException, IllegalAccessException{
		this.entityDef = entityDef;
		checkNotNull(entityDef, "serviceName");
	}
	
	private void checkNotNull(Object o, String message) {
		if(o == null){
			throw new IllegalArgumentException(message + " should not be null");
		}
	}

	/**
	 * @param entities
	 * @return row keys
	 * @throws Exception
	 */
	public List<String> write(List<? extends TaggedLogAPIEntity> entities) throws Exception{
		HBaseLogWriter writer = new HBaseLogWriter(entityDef.getTable(), entityDef.getColumnFamily());
		List<String> rowkeys = new ArrayList<String>(entities.size());
		List<InternalLog> logs = new ArrayList<InternalLog>(entities.size());
		
		try{
			writer.open();
			for(TaggedLogAPIEntity entity : entities){
				final InternalLog entityLog = HBaseInternalLogHelper.convertToInternalLog(entity, entityDef);
				logs.add(entityLog);
			}
			List<byte[]> bRowkeys  = writer.write(logs);
			for (byte[] rowkey : bRowkeys) {
				rowkeys.add(EagleBase64Wrapper.encodeByteArray2URLSafeString(rowkey));
			}

		}catch(Exception ex){
			LOG.error("fail writing tagged log", ex);
			throw ex;
		}finally{
			writer.close();
	 	}
		return rowkeys;
	}
}
