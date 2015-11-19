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
package org.apache.eagle.storage;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.eagle.storage.operation.RawQuery;

import java.io.IOException;
import java.util.List;

/**
 * Eagle DataStorage Interface
 *
 * Interface description:
 * 1) TaggedLogAPIEntity (Why not InternalLog: it's deeply hbase specific and all values are converted to byte[] which is unnecessary for other database like mysql)
 * 2) EntityDefinition
 * 3) Assume all ID are in type of String like encoded rowkey to hbase and UUID for RDBMS
 *
 * @since 3/18/15
 */
public interface DataStorage<I> {

    /**
     * Initialize when data storage is created
     *
     * @throws IOException
     */
    void init() throws IOException;

    /**
     * Execute update operation
     *
     * @throws IOException
     */
    <E extends TaggedLogAPIEntity> ModifyResult<I> update(List<E> entities, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute create operation
     *
     * @throws IOException
     */
    <E extends TaggedLogAPIEntity> ModifyResult<I> create(List<E> entities, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute delete operation by entities
     *
     * @param entities
     * @return
     * @throws IOException
     */
    <E extends TaggedLogAPIEntity> ModifyResult<I> delete(List<E> entities, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute delete operation by entity ids
     *
     * @throws IOException
     */
    ModifyResult<I> deleteByID(List<I> ids, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute delete operation
     *
     * @throws IOException
     */
    ModifyResult<I> delete(CompiledQuery query, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute query to return a list of results, may not always tagged entities
     *
     * @throws IOException
     */
    <E extends Object> QueryResult<E> query(CompiledQuery query, EntityDefinition entityDefinition) throws IOException;

    /**
     * Execute query to return a list of results
     * @param ids id set
     * @param entityDefinition entity definition
     * @param <E> result item type
     * @return QueryResult object
     * @throws IOException
     */
    <E extends Object> QueryResult<E> queryById(List<I> ids, EntityDefinition entityDefinition) throws IOException;

    /**
     * close data storage
     *
     * @throws IOException
     */
    void close() throws IOException;

    CompiledQuery compile(RawQuery query) throws QueryCompileException;
}