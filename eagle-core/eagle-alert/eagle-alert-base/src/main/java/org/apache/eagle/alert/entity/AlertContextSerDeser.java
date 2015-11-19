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
package org.apache.eagle.alert.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.eagle.common.metric.AlertContext;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.eagle.log.entity.meta.EntitySerDeser;

public class AlertContextSerDeser implements EntitySerDeser<AlertContext> {

	@Override
	public AlertContext deserialize(byte[] bytes) {
		AlertContext context = new AlertContext();
		Map<String, String> properties = new HashMap<String, String>();
		final int length = bytes.length;
		if (length < 4) { return context; }
		int size = Bytes.toInt(bytes, 0, 4);
		
		int offset = 4;
		for (int i = 0; i < size; i++) {
			int keySize =  Bytes.toInt(bytes, offset, 4);
			offset += 4;
			int valueSize =  Bytes.toInt(bytes, offset, 4);
			offset += 4;
			String key = Bytes.toString(bytes, offset, keySize);
			offset += keySize;
			String value =Bytes.toString(bytes, offset, valueSize);
			offset += valueSize;
			properties.put(key, value);
		}
		context.addAll(properties);
		return context;
	}

	@Override
	public byte[] serialize(AlertContext context) {
		
		final Map<String, String> pair = context.getProperties();
		int totalSize = 4;
		for (Entry<String, String> entry : pair.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
            int keySize = 0;
            if(key!=null) keySize = key.getBytes().length;
			int valueSize = 0;
            if(value!=null) valueSize = value.getBytes().length;
			totalSize += keySize + valueSize + 8;
		}
		byte[] buffer = new byte[totalSize];
		
		Bytes.putInt(buffer, 0, pair.size());
		int offset = 4;
		for (Entry<String, String> entry : pair.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			int keySize = key !=null ? key.getBytes().length : 0;
            int valueSize = value != null ? value.getBytes().length:0;

            Bytes.putInt(buffer, offset, keySize);
			offset += 4;
			Bytes.putInt(buffer, offset, valueSize);
			offset += 4;


            Bytes.putBytes(buffer, offset, key != null ? key.getBytes() : new byte[0], 0, keySize);
			offset += keySize;
			Bytes.putBytes(buffer, offset, value != null ? value.getBytes() : new byte[0], 0, valueSize);
			offset += valueSize;
		}
		return buffer;
	}


	@Override
	public Class<AlertContext> type(){
		return AlertContext.class;
	}
}
