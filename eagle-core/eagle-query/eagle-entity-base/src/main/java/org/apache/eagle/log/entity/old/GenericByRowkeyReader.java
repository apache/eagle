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
import java.util.List;
import java.util.Map;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogObjectMapper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.common.EagleBase64Wrapper;

public class GenericByRowkeyReader {
	private static final Logger LOG = LoggerFactory.getLogger(GenericByRowkeyReader.class);

	private TaggedLogObjectMapper mapper;
	private String table;
	private String columnFamily;
	private boolean outputAll;
	private List<String> outputColumns;
	private GenericReader.EntityFactory entityFactory;
	
	public GenericByRowkeyReader(TaggedLogObjectMapper mapper, GenericReader.EntityFactory entityFactory, String table, String columnFamily, boolean outputAll, List<String> outputColumns){
		this.mapper = mapper;
		this.entityFactory = entityFactory;
		this.table = table;
		this.columnFamily = columnFamily;
		this.outputAll = outputAll;
		this.outputColumns = outputColumns;
	}
	
	public List<TaggedLogAPIEntity> read(List<String> rowkeys) throws IOException{
		HBaseLogByRowkeyReader reader = new HBaseLogByRowkeyReader(this.table, this.columnFamily, 
				outputAll, outputColumns);
		List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
		try{
			reader.open();
			for(String rowkeyString : rowkeys){
				byte[] rowkey = EagleBase64Wrapper.decode(rowkeyString);
				InternalLog log = reader.get(rowkey);
				TaggedLogAPIEntity entity = entityFactory.create();
				entities.add(entity);
				entity.setTags(log.getTags());
				entity.setTimestamp(log.getTimestamp());
				entity.setEncodedRowkey(log.getEncodedRowkey());
				entity.setPrefix(log.getPrefix());
				Map<String, byte[]> qualifierValues = log.getQualifierValues();
				mapper.populateQualifierValues(entity, qualifierValues);
			}
		}catch(IOException ex){
			LOG.error("Fail read by rowkey", ex);
			throw ex;
		}finally{
			reader.close();
		}
		
		return entities;
	}
}
