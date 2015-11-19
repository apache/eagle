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
package org.apache.eagle.log.entity.old;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.base.taggedlog.TaggedLogObjectMapper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.common.DateTimeUtil;

public class GenericReader {
	private static final Logger LOG = LoggerFactory.getLogger(GenericReader.class);

	public interface EntityFactory{
		public TaggedLogAPIEntity create();
	}
	
	private Schema schema;
	private EntityFactory entityFactory;
	private TaggedLogObjectMapper mapper;
	
	public GenericReader(TaggedLogObjectMapper mapper, Schema schema, EntityFactory factory){
		this.mapper = mapper;
		this.schema = schema;
		this.entityFactory = factory;
	}
	
	public List<TaggedLogAPIEntity> read(String startTime, 
			String endTime, List<String> tagNameValues, List<String> outputTags, 
			List<String> outputFields, String startRowkey, int pageSize) throws Exception{
		Date start = DateTimeUtil.humanDateToDate(startTime);
		Date end = DateTimeUtil.humanDateToDate(endTime);
		
		// decode the query parameters
		// TODO should support one tag has multiple tag values
		Map<String, List<String>> searchTags = new HashMap<String, List<String>>();
		for(String tagNameValue : tagNameValues){
			String[] tmp = tagNameValue.split("=");
			if(tmp == null || tmp.length <=1){
				continue; // silently ignore this parameter
			}
			List<String> tagValues = searchTags.get(tmp[0]);
			if(tagValues == null){
				tagValues = new ArrayList<String>();
				searchTags.put(tmp[0], tagValues);
			}
			tagValues.add(tmp[1]);
		}
		
		int numTags = outputTags.size();
		int numFields = outputFields.size();
		byte[][] outputQualifiers = new byte[numTags+numFields][];
		int i = 0;
		for(String tag : outputTags){
			outputQualifiers[i++] = tag.getBytes();
		}
		for(String field : outputFields){
			outputQualifiers[i++] = field.getBytes();
		}
		// shortcut to avoid read when pageSize=0
		List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
		if(pageSize <= 0){
			return entities; // return empty entities
		}

		HBaseLogReader reader = new HBaseLogReader(schema, start, end, searchTags, startRowkey, outputQualifiers);
		try{
			reader.open();
			InternalLog log;
			int count = 0;
			while ((log = reader.read()) != null) {
				TaggedLogAPIEntity entity = entityFactory.create();
				entity.setTags(log.getTags());
				entity.setTimestamp(log.getTimestamp());
				entity.setEncodedRowkey(log.getEncodedRowkey());
				entity.setPrefix(log.getPrefix());
				entities.add(entity);
				
				Map<String, byte[]> qualifierValues = log.getQualifierValues();
				mapper.populateQualifierValues(entity, qualifierValues);
				if(++count == pageSize)
					break;
			}
		}catch(IOException ioe){
			LOG.error("Fail reading log", ioe);
			throw ioe;
		}finally{
			reader.close();
		}		
		return entities;
	}
}
