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
package org.apache.eagle.log.base.taggedlog;

import java.util.Map;

public interface TaggedLogObjectMapper {
	/**
	 * when read, business logic should convert schema-less key/value into business object based on its own schema
	 * @param entity
	 * @param qualifierValues
	 */
	public void populateQualifierValues(TaggedLogAPIEntity entity, Map<String, byte[]> qualifierValues);
	
	/**
	 * when write, business logic should convert business object to schema-less key value
	 * @param entity
	 * @return
	 */
	public Map<String, byte[]> createQualifierValues(TaggedLogAPIEntity entity);	
}
