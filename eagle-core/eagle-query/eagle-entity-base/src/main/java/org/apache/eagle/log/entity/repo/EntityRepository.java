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
package org.apache.eagle.log.entity.repo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntitySerDeser;

/**
 * Entity repository is used to store entity definition class. Each domain should define its own entities. Eagle entity
 * framework will scan all derived class implementations to get all entity definitions, then register them to EntityDefinitionManager.
 * 
 *
 */
public abstract class EntityRepository {

	protected Set<Class<? extends TaggedLogAPIEntity>> entitySet = new HashSet<Class<? extends TaggedLogAPIEntity>>();
	protected Map<Class<?>, EntitySerDeser<?>> serDeserMap = new HashMap<Class<?>, EntitySerDeser<?>>();

	public synchronized Collection<Class<? extends TaggedLogAPIEntity>> getEntitySet() {
		return new ArrayList<Class<? extends TaggedLogAPIEntity>>(entitySet);
	}
	
	public synchronized Map<Class<?>, EntitySerDeser<?>> getSerDeserMap() {
		return new HashMap<Class<?>, EntitySerDeser<?>>(serDeserMap);
	}
	
	public synchronized void registerEntity(Class<? extends TaggedLogAPIEntity> clazz) {
		entitySet.add(clazz);
	}

	public synchronized void registerSerDeser(Class<?> clazz, EntitySerDeser<?> serDeser) {
		serDeserMap.put(clazz, serDeser);
	}
	
}
