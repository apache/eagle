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

import org.apache.eagle.common.ByteUtil;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Serialization/deserialization for map type
 *
 */
@SuppressWarnings("rawtypes")
public class MapSerDeser implements EntitySerDeser<Map> {

	@SuppressWarnings({ "unchecked" })
	@Override
	public Map deserialize(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		final Map map = new TreeMap();
		int offset = 0;
		// get size of int array
		final int size = ByteUtil.bytesToInt(bytes, offset);
		offset += 4;
		
		for (int i = 0; i < size; ++i) {
			final int keyID = ByteUtil.bytesToInt(bytes, offset);
			offset += 4;
			final Class<?> keyClass = EntityDefinitionManager.getClassByID(keyID);
			if (keyClass == null) {
				throw new IllegalArgumentException("Unsupported key type ID: " + keyID);
			}
			final EntitySerDeser keySerDer = EntityDefinitionManager.getSerDeser(keyClass);
			final int keyLength = ByteUtil.bytesToInt(bytes, offset);
			offset += 4;
			final byte[] keyContent = new byte[keyLength];
			System.arraycopy(bytes, offset, keyContent, 0, keyLength);
			offset += keyLength;
			final Object key = keySerDer.deserialize(keyContent);
			
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
			
			map.put(key, value);
		}
		return map;
	}

	/**
	 *  size + key1 type ID + key1 length + key1 binary content + value1 type id + value length + value1 binary content + ...
	 *   4B        4B             4B             key1 bytes            4B              4B              value1 bytes
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public byte[] serialize(Map map) {
		if(map == null)
			return null;
		final int size = map.size();
		final int[] keyIDs = new int[size];
		final int[] valueIDs = new int[size];
		final byte[][] keyBytes = new byte[size][];
		final byte[][] valueBytes = new byte[size][];
		
		int totalSize = 4 + size * 16;
		int i = 0;
		Iterator iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			final Map.Entry entry = (Map.Entry)iter.next();
			final Object key = entry.getKey();
			final Object value = entry.getValue();
			Class<?> keyClass = key.getClass();
			Class<?> valueClass = NullObject.class;
			if (value != null) {
				valueClass = value.getClass();
			}
			int keyTypeID = EntityDefinitionManager.getIDBySerDerClass(keyClass);
			int valueTypeID = 0; // default null object
			if (valueClass != null) {
				valueTypeID = EntityDefinitionManager.getIDBySerDerClass(valueClass);
			}
			if (keyTypeID == -1) {
				if (key instanceof Map) {
					keyClass = Map.class;
					keyTypeID = EntityDefinitionManager.getIDBySerDerClass(keyClass);
				} else {
					throw new IllegalArgumentException("Unsupported class: " + keyClass.getName());
				}
			}
			if (valueTypeID == -1) {
				if (value instanceof Map) {
					valueClass = Map.class;
					valueTypeID = EntityDefinitionManager.getIDBySerDerClass(valueClass);
				} else {
					throw new IllegalArgumentException("Unsupported class: " + valueClass.getName());
				}
			}
			keyIDs[i] = keyTypeID;
			valueIDs[i] = valueTypeID;
			final EntitySerDeser keySerDer = EntityDefinitionManager.getSerDeser(keyClass);
			final EntitySerDeser valueSerDer = EntityDefinitionManager.getSerDeser(valueClass);
			if (keySerDer == null) {
				throw new IllegalArgumentException("Unsupported class: " + keyClass.getName());
			}
			if (valueSerDer == null) {
				throw new IllegalArgumentException("Unsupported class: " + valueClass.getName());
			}
			keyBytes[i] = keySerDer.serialize(key);
			valueBytes[i] = valueSerDer.serialize(value);
			totalSize += keyBytes[i].length + valueBytes[i].length;
			++i;
		}
		final byte[] result = new byte[totalSize];
		int offset = 0;
		ByteUtil.intToBytes(size, result, offset);
		offset += 4;
		for (i = 0; i < size; ++i) {
			ByteUtil.intToBytes(keyIDs[i], result, offset);
			offset += 4;
			ByteUtil.intToBytes(keyBytes[i].length, result, offset);
			offset += 4;
			System.arraycopy(keyBytes[i], 0, result, offset, keyBytes[i].length);
			offset += keyBytes[i].length;
			
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
	public Class<Map> type() {
		return Map.class;
	}
}

