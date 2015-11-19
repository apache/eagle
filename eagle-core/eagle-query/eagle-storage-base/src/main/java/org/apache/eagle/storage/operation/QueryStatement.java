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
package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.result.QueryResult;

import java.io.IOException;

/**
 * @since 3/18/15
 */
public class QueryStatement implements Statement<QueryResult<?>>{
    private final RawQuery query;

    public QueryStatement(RawQuery queryCondition){
        this.query = queryCondition;
    }

    @Override
    public QueryResult<?> execute(DataStorage dataStorage) throws IOException {
        CompiledQuery compiledQuery;
        try {
            compiledQuery = dataStorage.compile(this.query);
        } catch (QueryCompileException e) {
            throw new IOException(e);
        }
        try {
            EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(compiledQuery.getServiceName());
            return dataStorage.query(compiledQuery, entityDefinition);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException(e);
        }
    }
}