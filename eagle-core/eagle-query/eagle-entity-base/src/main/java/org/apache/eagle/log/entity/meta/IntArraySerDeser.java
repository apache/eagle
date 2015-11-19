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

/**
 * serialize int array which is stored like the following
 * <int><int>*size, where the first <int> is the size of int
 */
public class IntArraySerDeser implements EntitySerDeser<int[]>{

	public IntArraySerDeser(){}

	@Override
	public int[] deserialize(byte[] bytes){
		if(bytes.length < 4)
			return null;
		int offset = 0;
		// get size of int array
		int size = ByteUtil.bytesToInt(bytes, offset);
		offset += 4;
		int[] values = new int[size];
		for(int i=0; i<size; i++){
			values[i] = ByteUtil.bytesToInt(bytes, offset);
			offset += 4;
		}
		return values;
	}
	
	/**
	 * 
	 * @param obj
	 * @return
	 */
	@Override
	public byte[] serialize(int[] obj){
		if(obj == null)
			return null;
		int size = obj.length;
		byte[] array = new byte[4 + 4*size];
		byte[] first = ByteUtil.intToBytes(size);
		int offset = 0;
		System.arraycopy(first, 0, array, offset, first.length);
		offset += first.length;
		for(int i=0; i<size; i++){
			System.arraycopy(ByteUtil.intToBytes(obj[i]), 0, array, offset, 4);
			offset += 4;
		}
		return array;
	}

	@Override
	public Class<int[]> type() {
		return int[].class;
	}
}
