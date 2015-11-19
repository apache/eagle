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
import org.apache.eagle.storage.result.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @since 6/29/15
 */
public class RowkeyQueryStatement implements Statement<QueryResult<?>>{
    private final List<String> rowkeys;
    private final EntityDefinition entityDefinition;

    public RowkeyQueryStatement(String rowkey,EntityDefinition entityDefinition) {
        this.rowkeys = new ArrayList<>(1);
        this.rowkeys.add(rowkey);
        this.entityDefinition = entityDefinition;
    }

    public RowkeyQueryStatement(String rowkey,String serviceName) throws IllegalAccessException, InstantiationException {
        this.rowkeys = new ArrayList<>(1);
        this.rowkeys.add(rowkey);
        this.entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);
    }

    public RowkeyQueryStatement(List<String> rowkeys,EntityDefinition entityDefinition) {
        this.rowkeys = rowkeys;
        this.entityDefinition = entityDefinition;
    }

    public RowkeyQueryStatement(List<String> rowkeys,String serviceName) throws IllegalAccessException, InstantiationException {
        this.rowkeys = rowkeys;
        this.entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);
    }

    @Override
    public <I> QueryResult<?> execute(DataStorage<I> dataStorage) throws IOException {
        return dataStorage.queryById((List<I>) this.rowkeys,this.entityDefinition);
    }
}