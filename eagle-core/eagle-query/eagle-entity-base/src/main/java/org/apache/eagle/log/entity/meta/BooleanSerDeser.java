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

/**
 * @since : 7/3/14,2014
 */
public class BooleanSerDeser implements EntitySerDeser<Boolean> {

	public BooleanSerDeser(){}

	@Override
	public Boolean deserialize(byte[] bytes){
		if(bytes != null && bytes.length > 0){
			if(bytes[0] == 0){
				return false;
			}else if(bytes[0] == 1){
				return true;
			}
		}
		return null;
	}

	@Override
	public byte[] serialize(Boolean obj){
		if(obj != null){
			if(obj){
				return new byte[]{1};
			}else{
				return new byte[]{0};
			}
		}
		return null;
	}

	@Override
	public Class<Boolean> type() {
		return Boolean.class;
	}
}
