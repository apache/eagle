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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.entity.index.NonClusteredIndexStreamReader;
import org.apache.eagle.log.entity.index.UniqueIndexStreamReader;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.query.parser.ORExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GenericEntityStreamReader extends StreamReader {
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityStreamReader.class);
	
	private EntityDefinition entityDef;
	private SearchCondition condition;
	private String prefix;
	private StreamReader readerAfterPlan;

	public GenericEntityStreamReader(String serviceName, SearchCondition condition) throws InstantiationException, IllegalAccessException{
		this(serviceName, condition, null);
	}

	public GenericEntityStreamReader(EntityDefinition entityDef, SearchCondition condition) throws InstantiationException, IllegalAccessException{
		this(entityDef, condition, entityDef.getPrefix());
	}
	
	public GenericEntityStreamReader(String serviceName, SearchCondition condition, String prefix) throws InstantiationException, IllegalAccessException{
		this.prefix = prefix;
		checkNotNull(serviceName, "serviceName");
		this.entityDef = EntityDefinitionManager.getEntityByServiceName(serviceName);
		checkNotNull(entityDef, "EntityDefinition");
		this.condition = condition;
		this.readerAfterPlan = selectQueryReader();
	}

	public GenericEntityStreamReader(EntityDefinition entityDef, SearchCondition condition, String prefix) throws InstantiationException, IllegalAccessException{
		this.prefix = prefix;
		checkNotNull(entityDef, "entityDef");
		this.entityDef = entityDef;
		checkNotNull(entityDef, "EntityDefinition");
		this.condition = condition;
		this.readerAfterPlan = selectQueryReader();
	}

	private void checkNotNull(Object o, String message){
		if(o == null){
			throw new IllegalArgumentException(message + " should not be null");
		}
	}
	
	public EntityDefinition getEntityDefinition() {
		return entityDef;
	}
	
	public SearchCondition getSearchCondition() {
		return condition;
	}
	
	@Override
	public void readAsStream() throws Exception{
		readerAfterPlan._listeners.addAll(this._listeners);
		readerAfterPlan.readAsStream();
	}
	
	private StreamReader selectQueryReader() throws InstantiationException, IllegalAccessException {
		final ORExpression query = condition.getQueryExpression();
		IndexDefinition[] indexDefs = entityDef.getIndexes();

        // Index just works with query condition
		if (indexDefs != null && condition.getQueryExpression()!=null) {
			List<byte[]> rowkeys = new ArrayList<>();
			for (IndexDefinition index : indexDefs) {
				// Check unique index first
				if (index.isUnique()) {
					final IndexDefinition.IndexType type = index.canGoThroughIndex(query, rowkeys);
					if (!IndexDefinition.IndexType.NON_INDEX.equals(type)) {
						LOG.info("Selectd query unique index " + index.getIndexName() + " for query: " + condition.getQueryExpression());
						return new UniqueIndexStreamReader(index, condition, rowkeys);
					}
				}
			}
			for (IndexDefinition index : indexDefs) {
				// Check non-clustered index
				if (!index.isUnique()) {
					final IndexDefinition.IndexType type = index.canGoThroughIndex(query, rowkeys);
					if (!IndexDefinition.IndexType.NON_INDEX.equals(type)) {
						LOG.info("Selectd query non clustered index " + index.getIndexName() + " for query: " + condition.getQueryExpression().toString());
						return new NonClusteredIndexStreamReader(index, condition, rowkeys);
					}
				}
			}
		}
		return new GenericEntityScanStreamReader(entityDef, condition, this.prefix);
	}

	@Override
	public long getLastTimestamp() {
		return readerAfterPlan.getLastTimestamp();
	}

	@Override
	public long getFirstTimestamp() {
		return readerAfterPlan.getFirstTimestamp();
	}
}
