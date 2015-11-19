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
package org.apache.eagle.service.rowkey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.RowkeyAPIEntity;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.old.GenericDeleter;
import org.apache.eagle.log.entity.old.HBaseLogByRowkeyReader;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.eagle.common.service.POSTResultEntityBase;

@Deprecated
@Path("rowkey")
public class RowkeyResource {
	private static final Logger LOG = LoggerFactory.getLogger(RowkeyResource.class);
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public RowkeyAPIEntity inspectRowkey(@QueryParam("table") String table, @QueryParam("cf") String columnFamily, 
			@QueryParam("key") String key, @QueryParam("all") String all, @QueryParam("field") List<String> fields){
		RowkeyAPIEntity entity = new RowkeyAPIEntity();
		byte[] row = null;
		boolean includingAllQualifiers = false;
		if(all != null && all.equals("true"))
			includingAllQualifiers = true;
		HBaseLogByRowkeyReader getter = new HBaseLogByRowkeyReader(table, columnFamily, includingAllQualifiers, fields);
		InternalLog log = null;
		try{
			getter.open();
			row = EagleBase64Wrapper.decode(key);
			log = getter.get(row);
		}catch(Exception ex){
			LOG.error("Cannot get rowkey", ex);
			entity.setSuccess(false);
			entity.setException(EagleExceptionWrapper.wrap(ex));
			return entity;
		}finally{
			try{
				getter.close();
			}catch(Exception ex){}
		}
		
		Map<String, String> fieldNameValueMap = new TreeMap<String, String>();
		entity.setFieldNameValueMap(fieldNameValueMap);
		// populate qualifiers
		Map<String, byte[]> qualifierValues = log.getQualifierValues();
		for(Map.Entry<String, byte[]> qualifier : qualifierValues.entrySet()){
			if(qualifier.getValue() != null){
				fieldNameValueMap.put(qualifier.getKey(), new String(qualifier.getValue()));
			}
		}
		
		// decode rowkey
		// the first integer is prefix hashcode
		entity.setPrefixHashCode(ByteUtil.bytesToInt(row, 0));
		long ts = Long.MAX_VALUE-ByteUtil.bytesToLong(row, 4);
		entity.setTimestamp(ts);
		entity.setHumanTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(ts));
		int offset = 4+8;
		int len = row.length;
		Map<Integer, Integer> tagNameHashValueHashMap = new HashMap<Integer, Integer>();
		// TODO boundary check please
		while(offset < len){
			int tagNameHash = ByteUtil.bytesToInt(row, offset);
			offset += 4;
			int tagValueHash = ByteUtil.bytesToInt(row, offset);
			offset += 4;
			tagNameHashValueHashMap.put(tagNameHash, tagValueHash);
		}
		
		entity.setSuccess(true);
		return entity;
	}
	
	/**
	 * for entities, the only required field is encodedRowkey
	 * @param table
	 * @param columnFamily
	 * @param entities
	 */
	@DELETE
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public POSTResultEntityBase deleteEntityByEncodedRowkey(@QueryParam("table") String table, @QueryParam("cf") String columnFamily,
			List<TaggedLogAPIEntity> entities){
		GenericDeleter deleter = new GenericDeleter(table, columnFamily);
		POSTResultEntityBase result = new POSTResultEntityBase();
		try{
			deleter.delete(entities);
		}catch(Exception ex){
			LOG.error("Fail deleting entity " + table + ":" + columnFamily, ex);
			result.setSuccess(false);
			result.setException(ex.getMessage());
			return result;
		}
		result.setSuccess(true);
		return result;
	}
}
