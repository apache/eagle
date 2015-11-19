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
package org.apache.eagle.log.entity.index;

import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.LogReader;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.log.entity.meta.IndexDefinition.IndexType;

import java.util.ArrayList;
import java.util.List;

public class NonClusteredIndexStreamReader extends IndexStreamReader {
	public NonClusteredIndexStreamReader(IndexDefinition indexDef, SearchCondition condition) {
		super(indexDef, condition, new ArrayList<byte[]>());
		final IndexType type = indexDef.canGoThroughIndex(condition.getQueryExpression(), indexRowkeys);
		if (!IndexType.NON_CLUSTER_INDEX.equals(type)) {
			throw new IllegalArgumentException("This query can't go through index: " + condition.getQueryExpression());
		}
	}

	public NonClusteredIndexStreamReader(IndexDefinition indexDef, SearchCondition condition, List<byte[]> indexRowkeys) {
		super(indexDef, condition, indexRowkeys);
	}

	@Override
	protected LogReader createIndexReader() {
		final EntityDefinition entityDef = indexDef.getEntityDefinition();
		byte[][] outputQualifiers = null;
		if(!condition.isOutputAll()) {
			outputQualifiers = HBaseInternalLogHelper.getOutputQualifiers(entityDef, condition.getOutputFields());
		}
		return new NonClusteredIndexLogReader(indexDef, indexRowkeys, outputQualifiers, condition.getFilter());
	}
}
