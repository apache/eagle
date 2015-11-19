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
package org.apache.eagle.storage.hbase;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericEntityWriter;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.index.RowKeyLogReader;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.old.GenericDeleter;
import org.apache.eagle.query.GenericQuery;
import org.apache.eagle.storage.DataStorageBase;
import org.apache.eagle.storage.hbase.query.GenericQueryBuilder;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.eagle.common.EagleBase64Wrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @since 3/18/15
 */
public class HBaseStorage extends DataStorageBase {

    private final static Logger LOG = LoggerFactory.getLogger(HBaseStorage.class);

    @Override
    public void init() throws IOException {
        LOG.info("Initializing");
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> update(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        return create(entities, entityDefinition);
    }

    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> create(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<>();
        try {
            GenericEntityWriter entityWriter = new GenericEntityWriter(entityDefinition);
            result.setIdentifiers(entityWriter.write(entities));
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new IOException(e);
        }
        return result;
    }

    /**
     * @param entities
     * @param entityDefinition
     * @param <E>
     *
     * @return ModifyResult
     *
     * @throws IOException
     */
    @Override
    public <E extends TaggedLogAPIEntity> ModifyResult<String> delete(List<E> entities, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try{
            GenericDeleter deleter = new GenericDeleter(entityDefinition.getTable(), entityDefinition.getColumnFamily());
            result.setIdentifiers(deleter.delete(entities));
        }catch(Exception ex){
            LOG.error(ex.getMessage(),ex);
            result.setSuccess(false);
            throw new IOException(ex);
        }
        result.setSuccess(true);
        return result;
    }

    /**
     * TODO:
     *
     * @param ids
     * @param entityDefinition
     * @return
     * @throws IOException
     */
    @Override
    public ModifyResult<String> deleteByID(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        ModifyResult<String> result = new ModifyResult<String>();
        try{
            GenericDeleter deleter = new GenericDeleter(entityDefinition.getTable(), entityDefinition.getColumnFamily());
            deleter.deleteByEncodedRowkeys(ids);
            result.setIdentifiers(ids);
        }catch(Exception ex){
            LOG.error(ex.getMessage(),ex);
            result.setSuccess(false);
            throw new IOException(ex);
        }
        result.setSuccess(true);
        return result;
    }

    /**
     * TODO:
     *
     * @param query
     * @param entityDefinition
     * @return
     * @throws IOException
     */
    @Override
    public ModifyResult<String> delete(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        if(query.isHasAgg()){
            throw new IOException("delete by aggregation query is not supported");
        }
        ModifyResult<String> result;

        try {
            LOG.info("Querying for deleting: "+query);
            GenericQuery reader = GenericQueryBuilder
                    .select(query.getSearchCondition().getOutputFields())
                    .from(query.getServiceName(),query.getRawQuery().getMetricName()).where(query.getSearchCondition())
                    .groupBy(query.isHasAgg(), query.getGroupByFields(), query.getAggregateFunctionTypes(), query.getAggregateFields())
                    .timeSeries(query.getRawQuery().isTimeSeries(),query.getRawQuery().getIntervalmin())
                    .treeAgg(query.getRawQuery().isTreeAgg())
                    .orderBy(query.getSortOptions(),query.getSortFunctions(),query.getSortFields())
                    .top(query.getRawQuery().getTop())
                    .parallel(query.getRawQuery().getParallel())
                    .build();
            List<? extends TaggedLogAPIEntity> entities = reader.result();
            if(entities != null){
                LOG.info("Deleting "+entities.size()+" entities");
                result =  delete(entities,entityDefinition);
            }else{
                LOG.info("Deleting 0 entities");
                result = new ModifyResult<String>();
                result.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new IOException(e);
        }
        return result;
    }

    /**
     * TODO:
     *
     * @param query
     * @param entityDefinition
     * @param <E>
     * @return
     * @throws IOException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <E extends Object> QueryResult<E> query(CompiledQuery query, EntityDefinition entityDefinition) throws IOException {
        QueryResult<E> result = new QueryResult<E>();
        try {
            GenericQuery reader = GenericQueryBuilder
                    .select(query.getSearchCondition().getOutputFields())
                    .from(query.getServiceName(),query.getRawQuery().getMetricName()).where(query.getSearchCondition())
                    .groupBy(query.isHasAgg(), query.getGroupByFields(), query.getAggregateFunctionTypes(), query.getAggregateFields())
                    .timeSeries(query.getRawQuery().isTimeSeries(),query.getRawQuery().getIntervalmin())
                    .treeAgg(query.getRawQuery().isTreeAgg())
                    .orderBy(query.getSortOptions(),query.getSortFunctions(),query.getSortFields())
                    .top(query.getRawQuery().getTop())
                    .parallel(query.getRawQuery().getParallel())
                    .build();
            List<E> entities =  reader.result();
            result.setData(entities);
            result.setFirstTimestamp(reader.getFirstTimeStamp());
            result.setLastTimestamp(reader.getLastTimestamp());
            result.setSize(entities.size());
            if(!query.isHasAgg()) result.setEntityType((Class<E>) entityDefinition.getEntityClass());
            result.setSuccess(true);
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new IOException(e);
        }
        return result;
    }

    /**
     * Query by HBase rowkey
     *
     * @param ids hbase rowkey list
     * @param entityDefinition entity definition
     * @param <E> entity type
     * @return QueryResult with entity type <E>
     *
     * @throws IOException
     */
    @Override
    public <E> QueryResult<E> queryById(List<String> ids, EntityDefinition entityDefinition) throws IOException {
        List<byte[]> rowkeys = new ArrayList<>(ids.size());
        QueryResult<E> result = new QueryResult<E>();
        for(String id:ids) rowkeys.add(EagleBase64Wrapper.decode(id));
        RowKeyLogReader reader = null;
        try {
            reader = new RowKeyLogReader(entityDefinition, rowkeys,null);
            reader.open();
            List<TaggedLogAPIEntity> entities = new LinkedList<>();

            while(true) {
                InternalLog log = reader.read();
                if(log == null) break;
                TaggedLogAPIEntity entity = HBaseInternalLogHelper.buildEntity(log, entityDefinition);
                entities.add(entity);
            }

            result.setData((List<E>) entities);
            result.setSuccess(true);
            result.setSize(entities.size());
            return result;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IOException(e);
        } finally{
            if(reader != null) reader.close();
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Shutting down");
    }
}