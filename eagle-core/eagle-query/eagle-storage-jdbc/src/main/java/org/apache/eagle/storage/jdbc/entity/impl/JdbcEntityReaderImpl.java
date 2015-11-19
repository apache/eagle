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
package org.apache.eagle.storage.jdbc.entity.impl;

import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.apache.eagle.storage.jdbc.conn.impl.TorqueStatementPeerImpl;
import org.apache.eagle.storage.jdbc.criteria.impl.PrimaryKeyCriteriaBuilder;
import org.apache.eagle.storage.jdbc.criteria.impl.QueryCriteriaBuilder;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityReader;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.commons.lang.time.StopWatch;
import org.apache.torque.criteria.Criteria;
import org.apache.torque.om.mapper.RecordMapper;
import org.apache.torque.sql.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @since 3/27/15
 */
public class JdbcEntityReaderImpl implements JdbcEntityReader {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntityReaderImpl.class);
    private final JdbcEntityDefinition jdbcEntityDefinition;
    private long resultFirstTimestamp = 0;
    private long resultLastTimestamp = 0;

    public JdbcEntityReaderImpl(JdbcEntityDefinition jdbcEntityDefinition) {
        this.jdbcEntityDefinition = jdbcEntityDefinition;
    }

    @SuppressWarnings("unchecked")
    public <E extends Object> List<E> query(CompiledQuery query) throws Exception {
        QueryCriteriaBuilder criteriaBuilder = new QueryCriteriaBuilder(query,this.jdbcEntityDefinition.getJdbcTableName());
        Criteria criteria = criteriaBuilder.build();
        String displaySql = SqlBuilder.buildQuery(criteria).getDisplayString();

        if(LOG.isDebugEnabled()) LOG.debug("Querying: " + displaySql);

        RecordMapper<E> recordMapper;
        if(query.isHasAgg()) {
            recordMapper = (RecordMapper<E>) new AggreagteRecordMapper(query, jdbcEntityDefinition);
        }else{
            recordMapper = new EntityRecordMapper(jdbcEntityDefinition);
        }
        final StopWatch stopWatch = new StopWatch();
        List<E> result;
        try {
            stopWatch.start();
            TorqueStatementPeerImpl peer = ConnectionManagerFactory.getInstance().getStatementExecutor();
            result = peer.delegate().doSelect(criteria, recordMapper);
            LOG.info(String.format("Read %s records in %s ms (sql: %s)",result.size(),stopWatch.getTime(),displaySql));
        }catch (Exception ex){
            LOG.error("Failed to query by: "+displaySql+", due to: "+ex.getMessage(),ex);
            throw new IOException("Failed to query by: "+displaySql,ex);
        }finally {
            stopWatch.stop();
        }
        return result;
    }

    @Override
    public <E> List<E> query(List<String> ids) throws Exception {
        PrimaryKeyCriteriaBuilder criteriaBuilder = new PrimaryKeyCriteriaBuilder(ids,this.jdbcEntityDefinition.getJdbcTableName());
        Criteria criteria = criteriaBuilder.build();
        String displaySql = SqlBuilder.buildQuery(criteria).getDisplayString();
        if(LOG.isDebugEnabled()) LOG.debug("Querying: " + displaySql);
        EntityRecordMapper recordMapper = new EntityRecordMapper(jdbcEntityDefinition);
        final StopWatch stopWatch = new StopWatch();
        List<E> result;
        try {
            stopWatch.start();
            TorqueStatementPeerImpl peer = ConnectionManagerFactory.getInstance().getStatementExecutor();
            result = peer.delegate().doSelect(criteria, recordMapper);
            LOG.info(String.format("Read %s records in %s ms (sql: %s)",result.size(),stopWatch.getTime(),displaySql));
        }catch (Exception ex){
            LOG.error("Failed to query by: "+displaySql+", due to: "+ex.getMessage(),ex);
            throw new IOException("Failed to query by: "+displaySql,ex);
        }finally {
            stopWatch.stop();
        }
        return result;
    }

    public Long getResultFirstTimestamp() {
        return resultFirstTimestamp;
    }

    public Long getResultLastTimestamp() {
        return resultLastTimestamp;
    }
}