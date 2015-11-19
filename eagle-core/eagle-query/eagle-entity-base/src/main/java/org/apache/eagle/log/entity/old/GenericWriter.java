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
import java.util.TreeMap;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.base.taggedlog.TaggedLogObjectMapper;
import org.apache.eagle.log.entity.HBaseLogWriter;
import org.apache.eagle.log.entity.InternalLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.EagleBase64Wrapper;

public class GenericWriter {
	private static final Logger LOG = LoggerFactory.getLogger(GenericWriter.class);

	private String table;
	private String columnFamily;
	private TaggedLogObjectMapper mapper;
	
	public GenericWriter(TaggedLogObjectMapper mapper, String table, String columnFamily){
		this.mapper = mapper;
		this.table = table;
		this.columnFamily = columnFamily;
	}
	
	public List<String> write(List<? extends TaggedLogAPIEntity> entities) throws IOException{
		HBaseLogWriter writer = new HBaseLogWriter(table, columnFamily);
		List<String> rowkeys = new ArrayList<String>();
		
		try{
			writer.open();
			for(TaggedLogAPIEntity entity : entities){
				InternalLog log = new InternalLog();
				Map<String, String> inputTags = entity.getTags();
				Map<String, String> tags = new TreeMap<String, String>();
				for(Map.Entry<String, String> entry : inputTags.entrySet()){
					tags.put(entry.getKey(), entry.getValue());
				}
				log.setTags(tags);
				log.setTimestamp(entity.getTimestamp());
				log.setPrefix(entity.getPrefix());
				log.setQualifierValues(mapper.createQualifierValues(entity));
				byte[] rowkey  = writer.write(log);
				rowkeys.add(EagleBase64Wrapper.encodeByteArray2URLSafeString(rowkey));
			}
		}catch(IOException ioe){
			LOG.error("Fail writing tagged log", ioe);
			throw ioe;
		}finally{
			writer.close();
	 	}
		return rowkeys;
	}
	
	public void updateByRowkey(List<? extends TaggedLogAPIEntity> entities) throws IOException{
		HBaseLogWriter writer = new HBaseLogWriter(table, columnFamily);
		try{
			writer.open();
			for(TaggedLogAPIEntity entity : entities){
				byte[] rowkey = EagleBase64Wrapper.decode(entity.getEncodedRowkey());
				InternalLog log = new InternalLog();
				log.setQualifierValues(mapper.createQualifierValues(entity));
				writer.updateByRowkey(rowkey, log);
			}
		}catch(IOException ioe){
			LOG.error("Fail writing tagged log", ioe);
			throw ioe;
		}finally{
			writer.close();
	 	}
	}
}
