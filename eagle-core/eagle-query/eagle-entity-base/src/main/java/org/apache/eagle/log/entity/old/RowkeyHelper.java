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

import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.RowkeyBuilder;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.common.EagleBase64Wrapper;

public final class RowkeyHelper {

	public static byte[] getRowkey(TaggedLogAPIEntity entity, EntityDefinition entityDef) throws Exception {
		byte[] rowkey = null;
		if(entity.getEncodedRowkey() != null && !(entity.getEncodedRowkey().isEmpty())){
			rowkey = EagleBase64Wrapper.decode(entity.getEncodedRowkey());
		}else{
			InternalLog log = HBaseInternalLogHelper.convertToInternalLog(entity, entityDef);
			rowkey = RowkeyBuilder.buildRowkey(log);
		}
		return rowkey;
	}

	public static List<byte[]> getRowkeysByEntities(List<? extends TaggedLogAPIEntity> entities, EntityDefinition entityDef) throws Exception {
		final List<byte[]> result = new ArrayList<byte[]>(entities.size());
		for (TaggedLogAPIEntity entity : entities) {
			final byte[] rowkey = getRowkey(entity, entityDef);
			result.add(rowkey);
		}
		return result;
	}
	

	public static byte[] getRowkey(InternalLog log) {
		byte[] rowkey = null;
		if(log.getEncodedRowkey() != null && !(log.getEncodedRowkey().isEmpty())){
			rowkey = EagleBase64Wrapper.decode(log.getEncodedRowkey());
		}else{
			rowkey = RowkeyBuilder.buildRowkey(log);
		}
		return rowkey;
	}

	public static List<byte[]> getRowkeysByLogs(List<InternalLog> logs) {
		final List<byte[]> result = new ArrayList<byte[]>(logs.size());
		for (InternalLog log : logs) {
			final byte[] rowkey = getRowkey(log);
			result.add(rowkey);
		}
		return result;
	}

	public static byte[] getRowkey(String encodedRowkey) {
		byte[] rowkey = EagleBase64Wrapper.decode(encodedRowkey);
		return rowkey;
	}

	public static List<byte[]> getRowkeysByEncodedRowkeys(List<String> encodedRowkeys) {
		final List<byte[]> result = new ArrayList<byte[]>(encodedRowkeys.size());
		for (String encodedRowkey : encodedRowkeys) {
			byte[] rowkey = EagleBase64Wrapper.decode(encodedRowkey);
			result.add(rowkey);
		}
		return result;
	}

}
