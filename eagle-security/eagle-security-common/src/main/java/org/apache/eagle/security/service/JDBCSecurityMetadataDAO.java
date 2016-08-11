/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.service;

import com.google.inject.Inject;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

/**
 * Since 8/8/16.
 */
public class JDBCSecurityMetadataDAO implements ISecurityMetadataDAO  {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSecurityMetadataDAO.class);

    /**
     * composite primary key: site and hbase_resource
     */
    private final String TABLE_DDL_STATEMENT = "create table if not exists hbase_sensitivity_entity (site varchar(20), hbase_resource varchar(100), sensitivity_type varchar(20), primary key (site, hbase_resource));";
    private final String QUERY_ALL_STATEMENT = "SELECT site, hbase_resource, sensitivity_type FROM hbase_sensitivity_entity";
    private final String INSERT_STATEMENT = "INSERT INTO hbase_sensitivity_entity (site, hbase_resource, sensitivity_type) VALUES (?, ?, ?)";

    private DataSource dataSource;
    private JDBCMetadataQueryService queryService;

    /**
     * Inject datasource
     *
     * @param dataSource
     */
    @Inject
    public JDBCSecurityMetadataDAO(DataSource dataSource, JDBCMetadataQueryService queryService) {
        this.dataSource = dataSource;
        this.queryService = queryService;
        try {
            queryService.execute(TABLE_DDL_STATEMENT);
        } catch (SQLException e) {
            LOG.error("Unable to create table hbase_sensitivity_entity",e);
            throw new IllegalStateException("Unable to create table hbase_sensitivity_entity",e);
        }
    }

    @Override
    public Collection<HBaseSensitivityEntity> listHBaseSensitivies() {
        try {
            return queryService.query(QUERY_ALL_STATEMENT,(rs -> {
                HBaseSensitivityEntity entity = new HBaseSensitivityEntity();
                entity.setSite(rs.getString(1));
                entity.setHbaseResource(rs.getString(2));
                entity.setSensitivityType(rs.getString(3));
                return entity;
            }));
        } catch (SQLException e) {
            LOG.error("Error in querying all from hbase_sensitivity_entity table", e);
        }
        return Collections.emptyList();
    }

    @Override
    public OpResult addHBaseSensitivity(Collection<HBaseSensitivityEntity> h) {
        Connection connection = null;
        PreparedStatement statement = null;
        try{
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(INSERT_STATEMENT);
            connection.setAutoCommit(false);
            for(HBaseSensitivityEntity entity : h){
                statement.setString(1, entity.getSite());
                statement.setString(2, entity.getHbaseResource());
                statement.setString(3, entity.getSensitivityType());
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }catch(Exception ex){
            LOG.error("error in querying hbase_sensitivity_entity table", ex);
        }finally {
            try {
                if (statement != null)
                    statement.close();
                if(connection != null)
                    connection.close();
            }catch(Exception ex){
                LOG.error("error in closing database resources", ex);
            }
        }
        return null;
    }
}
