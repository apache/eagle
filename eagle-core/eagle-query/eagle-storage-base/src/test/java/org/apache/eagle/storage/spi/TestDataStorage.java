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
package org.apache.eagle.storage.spi;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.storage.DataStorageBase;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @since 3/20/15
 */
public class TestDataStorage extends DataStorageBase {

    @Override
    public void init() throws IOException {

    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> update(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> create(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> delete(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public ModifyResult<String> deleteByID(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public ModifyResult<String> delete(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public <E> QueryResult<E> query(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public <E> QueryResult<E> queryById(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Test
    public void test() {

    }
}
