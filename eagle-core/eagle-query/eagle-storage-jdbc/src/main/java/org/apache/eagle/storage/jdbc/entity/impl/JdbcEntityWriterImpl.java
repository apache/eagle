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
import org.apache.eagle.storage.jdbc.entity.JdbcEntitySerDeserHelper;
import org.apache.eagle.storage.jdbc.entity.JdbcEntityWriter;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.commons.lang.time.StopWatch;
import org.apache.torque.om.ObjectKey;
import org.apache.torque.util.ColumnValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @since 3/27/15
 */
public class JdbcEntityWriterImpl<E extends TaggedLogAPIEntity> implements JdbcEntityWriter<E> {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntityWriterImpl.class);

    private ConnectionManager connectionManager;
    private JdbcEntityDefinition jdbcEntityDefinition;

    public JdbcEntityWriterImpl(JdbcEntityDefinition jdbcEntityDefinition) {
        this.jdbcEntityDefinition = jdbcEntityDefinition;
        try {
            this.connectionManager = ConnectionManagerFactory.getInstance();
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> write(List<E> entities) throws Exception {
        List<String> keys = new ArrayList<String>();
        if(LOG.isDebugEnabled()) LOG.debug("Writing "+entities.size()+" entities");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Connection connection = ConnectionManagerFactory.getInstance().getConnection();
        // set auto commit false and commit by hands for 3x~5x better performance
        connection.setAutoCommit(false);

        try {
            TorqueStatementPeerImpl<E> peer = connectionManager.getStatementExecutor(this.jdbcEntityDefinition.getJdbcTableName());
            for (E entity : entities) {
                entity.setEncodedRowkey(peer.getPrimaryKeyBuilder().build(entity));
                ColumnValues columnValues = JdbcEntitySerDeserHelper.buildColumnValues(entity, this.jdbcEntityDefinition);

                // TODO: implement batch insert for better performance
                ObjectKey key = peer.delegate().doInsert(columnValues,connection);

                try {
                    if (key != null) {
                        keys.add((String) key.getValue());
                    } else {
                        keys.add(entity.getEncodedRowkey());
                    }
                } catch (ClassCastException ex) {
                    throw new RuntimeException("Key is not in type of String (VARCHAR) , but JdbcType (java.sql.Types): " + key.getJdbcType() + ", value: " + key.getValue(), ex);
                }
            }

            // Why not commit in finally: give up all if any single entity throws exception to make sure consistency guarantee
            if(LOG.isDebugEnabled()){
                LOG.debug("Committing writing");
            }
            connection.commit();
        }catch (Exception ex) {
            LOG.error("Failed to write records, rolling back",ex);
            connection.rollback();
            throw ex;
        }finally {
            stopWatch.stop();
            if(LOG.isDebugEnabled()) LOG.debug("Closing connection");
            connection.close();
        }

        LOG.info(String.format("Wrote %s records in %s ms (table: %s)",keys.size(),stopWatch.getTime(),this.jdbcEntityDefinition.getJdbcTableName()));
        return keys;
    }
}