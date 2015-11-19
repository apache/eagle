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
import org.apache.eagle.storage.jdbc.conn.ConnectionManager;
import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.apache.eagle.storage.jdbc.conn.impl.TorqueStatementPeerImpl;
import org.apache.eagle.storage.jdbc.criteria.impl.PrimaryKeyCriteriaBuilder;
import org.apache.eagle.storage.jdbc.entity.JdbcEntitySerDeserHelper;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityUpdater;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.commons.lang.time.StopWatch;
import org.apache.torque.criteria.Criteria;
import org.apache.torque.sql.SqlBuilder;
import org.apache.torque.util.ColumnValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

/**
 * @since 3/27/15
 */
public class JdbcEntityUpdaterImpl<E extends TaggedLogAPIEntity> implements JdbcEntityUpdater<E> {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntityUpdaterImpl.class);
    private final JdbcEntityDefinition jdbcEntityDefinition;

    public JdbcEntityUpdaterImpl(JdbcEntityDefinition jdbcEntityDefinition) {
        this.jdbcEntityDefinition = jdbcEntityDefinition;
    }

    @Override
    public int update(List<E> entities) throws Exception {
        ConnectionManager cm = ConnectionManagerFactory.getInstance();
        TorqueStatementPeerImpl<E> peer = cm.getStatementExecutor(this.jdbcEntityDefinition.getJdbcTableName());
        Connection connection = cm.getConnection();
        connection.setAutoCommit(false);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int num = 0;
        try {
            for (E entity : entities) {
                String primaryKey = entity.getEncodedRowkey();
                PrimaryKeyCriteriaBuilder pkBuilder = new PrimaryKeyCriteriaBuilder(Arrays.asList(primaryKey), this.jdbcEntityDefinition.getJdbcTableName());
                Criteria selectCriteria = pkBuilder.build();
                if(LOG.isDebugEnabled()) LOG.debug("Updating by query: "+SqlBuilder.buildQuery(selectCriteria).getDisplayString());
                ColumnValues columnValues = JdbcEntitySerDeserHelper.buildColumnValues(entity, this.jdbcEntityDefinition);
                num += peer.delegate().doUpdate(selectCriteria, columnValues, connection);
            }
            if(LOG.isDebugEnabled()) LOG.debug("Committing updates");
            connection.commit();
        } catch (Exception ex) {
            LOG.error("Failed to update, rolling back",ex);
            connection.rollback();
            throw ex;
        }finally {
            stopWatch.stop();
            if(LOG.isDebugEnabled()) LOG.debug("Closing connection");
            connection.close();
        }
        LOG.info(String.format("Updated %s records in %s ms",num,stopWatch.getTime()));
        return num;
    }
}