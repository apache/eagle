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
package org.apache.eagle.log.entity.meta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.eagle.common.ByteUtil;

/**
 * Serialization/deserialization for map type
 *
 */
@SuppressWarnings("rawtypes")
public class ListSerDeser implements EntitySerDeser<List> {

	@SuppressWarnings({ "unchecked" })
	@Override
	public List deserialize(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		final List list = new ArrayList();
		int offset = 0;
		// get size of int array
		final int size = ByteUtil.bytesToInt(bytes, offset);
		offset += 4;
		
		for (int i = 0; i < size; ++i) {
			final int valueID = ByteUtil.bytesToInt(bytes, offset);
			offset += 4;
			final Class<?> valueClass = EntityDefinitionManager.getClassByID(valueID);
			if (valueClass == null) {
				throw new IllegalArgumentException("Unsupported value type ID: " + valueID);
			}
			final EntitySerDeser valueSerDer = EntityDefinitionManager.getSerDeser(valueClass);
			final int valueLength = ByteUtil.bytesToInt(bytes, offset);
			offset += 4;
			final byte[] valueContent = new byte[valueLength];
			System.arraycopy(bytes, offset, valueContent, 0, valueLength);
			offset += valueLength;
			final Object value = valueSerDer.deserialize(valueContent);
			
			list.add(value);
		}
		return list;
	}

	/**
	 *  size + value1 type id + value length + value1 binary content + ...
	 *   4B         4B              4B              value1 bytes
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public byte[] serialize(List list) {
		if(list == null)
			return null;
		final int size = list.size();
		final int[] valueIDs = new int[size];
		final byte[][] valueBytes = new byte[size][];
		
		int totalSize = 4 + size * 8;
		int i = 0;
		Iterator iter = list.iterator();
		while (iter.hasNext()) {
			final Object value = iter.next();
			Class<?> valueClass = value.getClass();
			int valueTypeID = EntityDefinitionManager.getIDBySerDerClass(valueClass);

			if (valueTypeID == -1) {
				if (value instanceof List) {
					valueClass = List.class;
					valueTypeID = EntityDefinitionManager.getIDBySerDerClass(valueClass);
				}
				else if (value instanceof Map) {
					valueClass = Map.class;
					valueTypeID = EntityDefinitionManager.getIDBySerDerClass(valueClass);
				}
				else {
					throw new IllegalArgumentException("Unsupported class: " + valueClass.getName());
				}
			}
			valueIDs[i] = valueTypeID;
			final EntitySerDeser valueSerDer = EntityDefinitionManager.getSerDeser(valueClass);
			if (valueSerDer == null) {
				throw new IllegalArgumentException("Unsupported class: " + valueClass.getName());
			}
			valueBytes[i] = valueSerDer.serialize(value);
			totalSize += valueBytes[i].length;
			++i;
		}
		final byte[] result = new byte[totalSize];
		int offset = 0;
		ByteUtil.intToBytes(size, result, offset);
		offset += 4;
		for (i = 0; i < size; ++i) {			
			ByteUtil.intToBytes(valueIDs[i], result, offset);
			offset += 4;
			ByteUtil.intToBytes(valueBytes[i].length, result, offset);
			offset += 4;
			System.arraycopy(valueBytes[i], 0, result, offset, valueBytes[i].length);
			offset += valueBytes[i].length;
		}
		return result;
	}

	@Override
	public Class<List> type() {
		return List.class;
	}
}

