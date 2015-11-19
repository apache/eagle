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
package org.apache.eagle.service.security.hive.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * query hive server jdbc interface to get database, tables, columns from metastore database
 * NOTE: we can get all databases through executing "show databases" in any of the database. "default" is always the database
 * all metadata is retrieved on the fly without local cache
 * sample connection:
 * !connect jdbc:hive2://localhost:10000/xademo hive hive org.apache.hive.jdbc.HiveDriver
 */
public class HiveMetadataByHiveServer2DAOImpl implements HiveMetadataDAO{
    private static Logger LOG = LoggerFactory.getLogger(HiveMetadataByHiveServer2DAOImpl.class);

    private HiveMetadataAccessConfig config;

    public HiveMetadataByHiveServer2DAOImpl(HiveMetadataAccessConfig config){
        this.config = config;
    }

    private Connection createConnection() throws Exception{
        Class.forName(config.getJdbcDriverClassName());
        Connection connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
        return connection;
    }

    @Override
    public List<String> getDatabases() throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try{
            connection = createConnection();
            statement = createConnection().createStatement();
            String sql = "show databases";
            rs = statement.executeQuery(sql);
            List<String> databases = new ArrayList<String>();
            while(rs.next()){
                databases.add(rs.getString("database_name"));
            }
            return databases;
        }catch(Exception ex){
            LOG.error("fail to get databases", ex);
            throw ex;
        }finally{
            if(connection != null) connection.close();
            if(statement != null) statement.close();
            if(rs != null) rs.close();
        }
    }

    @Override
    public List<String> getTables(String database) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try{
            connection = createConnection();
            statement = createConnection().createStatement();
            statement.execute("use " + database);
            String sql = "show tables";
            rs = statement.executeQuery(sql);
            List<String> tables = new ArrayList<String>();
            while(rs.next()){
                tables.add(rs.getString("tab_name"));
            }
            return tables;
        }catch(Exception ex){
            LOG.error("fail to get tables for database " + database, ex);
            throw ex;
        }finally{
            if(connection != null) connection.close();
            if(statement != null) statement.close();
            if(rs != null) rs.close();
        }
    }

    @Override
    public List<String> getColumns(String database, String table) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try{
            connection = createConnection();
            statement = createConnection().createStatement();
            statement.execute("use " + database);
            String sql = "desc " + table;
            rs = statement.executeQuery(sql);
            List<String> columns = new ArrayList<String>();
            while(rs.next()){
                columns.add(rs.getString("col_name"));
            }
            return columns;
        }catch(Exception ex){
            LOG.error("fail to get columns for database/table " + database + "/" + table , ex);
            throw ex;
        }finally{
            if(connection != null) connection.close();
            if(statement != null) statement.close();
            if(rs != null) rs.close();
        }
    }
}
