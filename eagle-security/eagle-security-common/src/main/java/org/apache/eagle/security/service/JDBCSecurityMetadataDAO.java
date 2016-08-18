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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Since 8/8/16.
 */
public class JDBCSecurityMetadataDAO implements ISecurityMetadataDAO  {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSecurityMetadataDAO.class);

    private Config config;
    /**
     * composite primary key: site and hbase_resource
     */
    private final String HBASE_QUERY_ALL_STATEMENT = "SELECT site, hbase_resource, sensitivity_type FROM hbase_sensitivity_entity";
    private final String HBASE_INSERT_STATEMENT = "INSERT INTO hbase_sensitivity_entity (site, hbase_resource, sensitivity_type) VALUES (?, ?, ?)";

    private final String HDFS_QUERY_ALL_STATEMENT = "SELECT site, filedir, sensitivity_type FROM hdfs_sensitivity_entity";
    private final String HDFS_INSERT_STATEMENT = "INSERT INTO hdfs_sensitivity_entity (site, filedir, sensitivity_type) VALUES (?, ?, ?)";

    private final String IPZONE_QUERY_ALL_STATEMENT = "SELECT iphost, security_zone FROM ip_securityzone";
    private final String IPZONE_INSERT_STATEMENT = "INSERT INTO ip_securityzone (iphost, security_zone) VALUES (?, ?, ?)";

    private final String HIVE_QUERY_ALL_STATEMENT = "SELECT site, hive_resource, sensitivity_type FROM hive_sensitivity_entity";
    private final String HIVE_INSERT_STATEMENT = "INSERT INTO hive_sensitivity_entity (site, hive_resource, sensitivity_type) VALUES (?, ?, ?)";

    // get connection url from config
    public JDBCSecurityMetadataDAO(Config config){
        this.config = config;
    }

    private Collection listEntities(String query, Function<ResultSet, Object> selectFun){
        Connection connection = null;
        PreparedStatement statement = null;
        Collection ret = new ArrayList<>();
        ResultSet rs = null;
        try {
            connection = getJdbcConnection();
            statement = connection.prepareStatement(query);
            rs = statement.executeQuery();
            while (rs.next()) {
                ret.add(selectFun.apply(rs));
            }
        }catch(Exception e) {
            LOG.error("error in querying table with query {}", query, e);
        }finally{
            try{
                if(rs != null)
                    rs.close();
                if(statement != null)
                    statement.close();
                if(connection != null)
                    connection.close();
            }catch(Exception ex){
                LOG.error("error in closing database resources", ex);
            }
        }
        return ret;
    }

    private OpResult addEntities(String query, Collection h, BiFunction<Object, PreparedStatement, PreparedStatement> setFunc){
        Connection connection = null;
        PreparedStatement statement = null;
        try{
            connection = getJdbcConnection();
            statement = connection.prepareStatement(query);
            connection.setAutoCommit(false);
            for(Object entity : h){
                setFunc.apply(entity, statement);
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }catch(Exception ex){
            LOG.error("error in querying hdfs_sensitivity_entity table", ex);
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
        return new OpResult();
    }

    @Override
    public Collection<HBaseSensitivityEntity> listHBaseSensitivities() {
        return listEntities(HBASE_QUERY_ALL_STATEMENT, rs -> {
            try {
                HBaseSensitivityEntity entity = new HBaseSensitivityEntity();
                entity.setSite(rs.getString(1));
                entity.setHbaseResource(rs.getString(2));
                entity.setSensitivityType(rs.getString(3));
                return entity;
            }catch(Exception ex){ throw new IllegalStateException(ex);}
        });
    }


    @Override
    public OpResult addHBaseSensitivity(Collection<HBaseSensitivityEntity> h) {
        return addEntities(HBASE_INSERT_STATEMENT, h, (entity, statement) -> {
            HBaseSensitivityEntity e = (HBaseSensitivityEntity)entity;
            try {
                statement.setString(1, e.getSite());
                statement.setString(2, e.getHbaseResource());
                statement.setString(3, e.getSensitivityType());
            }catch(Exception ex){
                throw new IllegalStateException(ex);
            }
            return statement;
        });
    }

    @Override
    public Collection<HdfsSensitivityEntity> listHdfsSensitivities() {
        return listEntities(HDFS_QUERY_ALL_STATEMENT, rs -> {
            try {
                HdfsSensitivityEntity entity = new HdfsSensitivityEntity();
                entity.setSite(rs.getString(1));
                entity.setFiledir(rs.getString(2));
                entity.setSensitivityType(rs.getString(3));
                return entity;
            }catch(Exception ex){ throw new IllegalStateException(ex);}
        });
    }

    @Override
    public OpResult addHdfsSensitivity(Collection<HdfsSensitivityEntity> h) {
        return addEntities(HDFS_INSERT_STATEMENT, h, (entity, statement) -> {
            HdfsSensitivityEntity e = (HdfsSensitivityEntity)entity;
            try {
                statement.setString(1, e.getSite());
                statement.setString(2, e.getFiledir());
                statement.setString(3, e.getSensitivityType());
            }catch(Exception ex){
                throw new IllegalStateException(ex);
            }
            return statement;
        });
    }

    @Override
    public Collection<IPZoneEntity> listIPZones() {
        return listEntities(IPZONE_QUERY_ALL_STATEMENT, rs -> {
            try {
                IPZoneEntity entity = new IPZoneEntity();
                entity.setIphost(rs.getString(1));
                entity.setSecurityZone(rs.getString(2));
                return entity;
            }catch(Exception ex){ throw new IllegalStateException(ex);}
        });
    }

    @Override
    public OpResult addIPZone(Collection<IPZoneEntity> h) {
        return addEntities(IPZONE_INSERT_STATEMENT, h, (entity, statement) -> {
            IPZoneEntity e = (IPZoneEntity)entity;
            try {
                statement.setString(1, e.getIphost());
                statement.setString(2, e.getSecurityZone());
            }catch(Exception ex){
                throw new IllegalStateException(ex);
            }
            return statement;
        });
    }

    @Override
    public Collection<HiveSensitivityEntity> listHiveSensitivities() {
        return listEntities(HIVE_QUERY_ALL_STATEMENT, rs -> {
            try {
                HiveSensitivityEntity entity = new HiveSensitivityEntity();
                entity.setSite(rs.getString(1));
                entity.setHiveResource(rs.getString(2));
                entity.setSensitivityType(rs.getString(3));
                return entity;
            }catch(Exception ex){ throw new IllegalStateException(ex);}
        });
    }

    @Override
    public OpResult addHiveSensitivity(Collection<HiveSensitivityEntity> h) {
        return addEntities(HIVE_INSERT_STATEMENT, h, (entity, statement) -> {
            HiveSensitivityEntity e = (HiveSensitivityEntity)entity;
            try {
                statement.setString(1, e.getSite());
                statement.setString(2, e.getHiveResource());
                statement.setString(3, e.getSensitivityType());
            }catch(Exception ex){
                throw new IllegalStateException(ex);
            }
            return statement;
        });
    }

    private Connection getJdbcConnection() throws Exception {
        Connection connection;
        try {
            connection = DriverManager.getConnection(config.getString("metadata.jdbc.url"),
                    config.getString("metadata.jdbc.username"),
                    config.getString("metadata.jdbc.password"));
        } catch (Exception e) {
            LOG.error("error get connection for {}", config.getString("metadata.jdbc.url"), e);
            throw e;
        }
        return connection;
    }
}
