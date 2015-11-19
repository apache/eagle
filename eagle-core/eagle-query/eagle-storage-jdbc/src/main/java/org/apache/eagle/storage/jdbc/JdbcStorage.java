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
package org.apache.eagle.storage.jdbc;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.storage.DataStorageBase;
import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityDeleter;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityReader;
import org.apache.eagle.storage.jdbc.entity.impl.JdbcEntityDeleterImpl;
import org.apache.eagle.storage.jdbc.entity.impl.JdbcEntityReaderImpl;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityUpdater;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityWriter;
import org.apache.eagle.storage.jdbc.entity.impl.JdbcEntityUpdaterImpl;
import org.apache.eagle.storage.jdbc.entity.impl.JdbcEntityWriterImpl;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinitionManager;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @since 3/18/15
 */
@SuppressWarnings("unchecked")
public class JdbcStorage extends DataStorageBase {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcStorage.class);

    @Override
    public void init() throws IOException {
        try {
            JdbcEntityDefinitionManager.load();
            ConnectionManagerFactory.getInstance();
        } catch (Exception e) {
            LOG.error("Failed to initialize connection manager",e);
            throw new IOException(e);
        }
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> update(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityUpdater updater = new JdbcEntityUpdaterImpl(jdbcEntityDefinition);
            int updated = updater.update(entities);
            result.setSize(updated);
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> create(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityWriter writer = new JdbcEntityWriterImpl(jdbcEntityDefinition);
            List<String> keys = writer.write(entities);
            result.setIdentifiers(keys);
            result.setSize(keys.size());
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> delete(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityDeleter writer = new JdbcEntityDeleterImpl(jdbcEntityDefinition);
            int num = writer.delete(entities);
            result.setSize(num);
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ModifyResult<String> deleteByID(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityDeleter writer = new JdbcEntityDeleterImpl(jdbcEntityDefinition);
            int num = writer.deleteByIds(ids);
            result.setSize(num);
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public ModifyResult<String> delete(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityDeleter writer = new JdbcEntityDeleterImpl(jdbcEntityDefinition);
            int num = writer.deleteByQuery(query);
            result.setSize(num);
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E extends Object> QueryResult<E> query(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        QueryResult<E> result = new QueryResult<E>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityReader reader = new JdbcEntityReaderImpl(jdbcEntityDefinition);
            List<E> entities = reader.query(query);
            result.setData(entities);
            if(entities!=null) {
                result.setSize(entities.size());
            }else{
                result.setSize(0);
            }
            if(query.isHasAgg()){
                result.setEntityType((Class<E>) Map.class);
            }else {
                result.setEntityType((Class<E>) entityDefinition.getEntityClass());
            }
            result.setFirstTimestamp(reader.getResultFirstTimestamp());
            result.setLastTimestamp(reader.getResultLastTimestamp());
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public <E> QueryResult<E> queryById(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        QueryResult<E> result = new QueryResult<E>();
        try {
            JdbcEntityDefinition jdbcEntityDefinition =  JdbcEntityDefinitionManager.getJdbcEntityDefinition(entityDefinition);
            JdbcEntityReader reader = new JdbcEntityReaderImpl(jdbcEntityDefinition);
            List<E> entities = reader.query(ids);
            result.setData(entities);
            if(entities!=null) {
                result.setSize(entities.size());
            }else{
                result.setSize(0);
            }
            result.setEntityType((Class<E>) entityDefinition.getEntityClass());
            result.setFirstTimestamp(reader.getResultFirstTimestamp());
            result.setLastTimestamp(reader.getResultLastTimestamp());
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            result.setSuccess(false);
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        try {
            ConnectionManagerFactory.getInstance().shutdown();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}