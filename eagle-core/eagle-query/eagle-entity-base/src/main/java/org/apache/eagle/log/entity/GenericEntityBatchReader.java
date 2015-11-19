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

public class GenericEntityBatchReader implements EntityCreationListener{
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityBatchReader.class);
	
	private List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
	private StreamReader reader;
	
	public GenericEntityBatchReader(String serviceName, SearchCondition condition) throws InstantiationException, IllegalAccessException{
		reader = new GenericEntityStreamReader(serviceName, condition);
		reader.register(this);
	}
	
	public GenericEntityBatchReader(StreamReader reader) throws InstantiationException, IllegalAccessException{
		this.reader = reader;
		reader.register(this);
	}
	
	public long getLastTimestamp() {
		return reader.getLastTimestamp();
	}
	public long getFirstTimestamp(){ return reader.getFirstTimestamp();}
	
	@Override
	public void entityCreated(TaggedLogAPIEntity entity){
		entities.add(entity);
	}
	
	@SuppressWarnings("unchecked")
	public <T> List<T> read() throws Exception{
		if(LOG.isDebugEnabled()) LOG.debug("Start reading as batch mode");
		reader.readAsStream();
		return (List<T>)entities;
	}
}
