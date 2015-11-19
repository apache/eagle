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

import java.io.UnsupportedEncodingException;

import org.apache.eagle.common.ByteUtil;

/**
 * String array entity serializer and deserializer
 *
 */
public class StringArraySerDeser implements EntitySerDeser<String[]> {

	public static final int MAX_STRING_LENGTH = 65535;
	public static final String UTF_8 = "UTF-8";
	
	@Override
	public String[] deserialize(byte[] bytes) {
		if(bytes == null || bytes.length < 4)
			return null;
		int offset = 0;
		// get size of int array
		final int size = ByteUtil.bytesToInt(bytes, offset);
		offset += 4;
		final String[] strings = new String[size];
		try {
			for(int i = 0; i < size; i++) {
				final int len = ByteUtil.bytesToInt(bytes, offset);
				offset += 4;
				strings[i] = new String(bytes, offset, len, UTF_8);
				offset += len;
			}
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Invalid byte array");
		}
		return strings;
	}
	
	/**
	 *  size + str1 length + str1 + str2 length + str2 + ...
	 *   4B        4B         n1B        4B        n2B
	 *  
	 * @param obj
	 * @return
	 */
	@Override
	public byte[] serialize(String[] array) {
		if(array == null)
			return null;
		final int size = array.length;
		final byte[][] tmp = new byte[size][];
		int total = 4 + 4 * size;
		for (int i = 0; i < size; ++i) {
			try {
				tmp[i] = array[i].getBytes(UTF_8);
			} catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException("String doesn't support UTF-8 encoding: " + array[i]);
			}
			total += tmp[i].length;
		}
		final byte[] result = new byte[total];
		int offset = 0;
		ByteUtil.intToBytes(size, result, offset);
		offset += 4;
		for (int i = 0; i < size; ++i) {
			ByteUtil.intToBytes(tmp[i].length, result, offset);
			offset += 4;
			System.arraycopy(tmp[i], 0, result, offset, tmp[i].length);
			offset += tmp[i].length;
		}
		return result;
	}

	@Override
	public Class<String[]> type() {
		return String[].class;
	}

}
