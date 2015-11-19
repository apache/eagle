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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.apache.eagle.storage.jdbc.conn.impl.TorqueStatementPeerImpl;
import org.apache.eagle.storage.jdbc.criteria.impl.PrimaryKeyCriteriaBuilder;
import org.apache.eagle.storage.jdbc.criteria.impl.QueryCriteriaBuilder;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityDeleter;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.commons.lang.time.StopWatch;
import org.apache.torque.criteria.Criteria;
import org.apache.torque.sql.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * @since 3/27/15
 */
public class JdbcEntityDeleterImpl<E extends TaggedLogAPIEntity> implements JdbcEntityDeleter<E> {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntityDeleterImpl.class);
    private final JdbcEntityDefinition jdbcEntityDefinition;
    public JdbcEntityDeleterImpl(JdbcEntityDefinition jdbcEntityDefinition) {
        this.jdbcEntityDefinition = jdbcEntityDefinition;
    }

    @Override
    public int delete(List<E> entities) throws Exception {
        List<String> primaryKeys = new LinkedList<String>();
        for(E e:entities){
            primaryKeys.add(e.getEncodedRowkey());
        }
        return deleteByIds(primaryKeys);
    }

    @Override
    public int deleteByIds(List<String> ids) throws Exception {
        PrimaryKeyCriteriaBuilder primaryKeyCriteriaBuilder = new PrimaryKeyCriteriaBuilder(ids,this.jdbcEntityDefinition.getJdbcTableName());
        Criteria criteria = primaryKeyCriteriaBuilder.build();
        return deleteByCriteria(criteria);
    }

    @Override
    public int deleteByQuery(CompiledQuery query) throws Exception {
        QueryCriteriaBuilder criteriaBuilder = new QueryCriteriaBuilder(query,this.jdbcEntityDefinition.getJdbcTableName());
        Criteria criteria = criteriaBuilder.build();
        return deleteByCriteria(criteria);
    }

    private int deleteByCriteria(Criteria criteria) throws Exception {
        String displaySql = SqlBuilder.buildQuery(criteria).getDisplayString();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        if(LOG.isDebugEnabled()) LOG.debug("Deleting by query: " + displaySql);
        try {
            TorqueStatementPeerImpl peer = ConnectionManagerFactory.getInstance().getStatementExecutor();
            int result = peer.delegate().doDelete(criteria);
            LOG.info(String.format("Deleted %s records in %s ms (sql: %s)",result,stopWatch.getTime(),displaySql));
            return result;
        } catch (Exception e) {
            LOG.error("Failed to delete by query: "+displaySql,e);
            throw e;
        } finally {
            stopWatch.stop();
        }
    }
}