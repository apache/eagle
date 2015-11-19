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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * directly query hive metadata including databases, tables, columns from metastore database
 * all metadata is retrieved on the fly without local cache
 */
public class HiveMetadataByMetastoreDBDAOImpl implements HiveMetadataDAO {
    private HiveMetadataAccessConfig config;
    public HiveMetadataByMetastoreDBDAOImpl(HiveMetadataAccessConfig config){
        this.config = config;
    }

    private Connection getConnection() throws Exception{
        Class.forName(config.getJdbcDriverClassName());
        Connection connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
        return connection;
    }

    @Override
    public List<String> getDatabases() throws Exception{
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String sql = "select name from DBS";
        ResultSet rs = statement.executeQuery(sql);
        List<String> databases = new ArrayList<String>();
        while(rs.next()){
            databases.add(rs.getString("name"));
        }
        if(connection != null) connection.close();
        if(statement != null) statement.close();
        if(rs != null) rs.close();
        return databases;
    }

    @Override
    public List<String> getTables(String database) throws Exception{
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String sql_format = "select t.tbl_name from TBLS t, DBS d where t.db_id=d.db_id and d.name='%s'";
        String sql = String.format(sql_format, database);
        ResultSet rs = statement.executeQuery(sql);
        List<String> tables = new ArrayList<String>();
        while(rs.next()){
            tables.add(rs.getString("tbl_name"));
        }
        if(connection != null) connection.close();
        if(statement != null) statement.close();
        if(rs != null) rs.close();
        return tables;
    }

    @Override
    public List<String> getColumns(String database, String table) throws Exception{
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String sql_format =
                "select c.column_name " +
                        "from DBS d join TBLS t on d.db_id=t.db_id " +
                        "join SDS s on t.sd_id=s.sd_id " +
                        "join COLUMNS_V2 c on s.cd_id=c.cd_id " +
                        "where d.name='%s' and t.tbl_name='%s';";
        String sql = String.format(sql_format, database, table);
        ResultSet rs = statement.executeQuery(sql);
        List<String> columns = new ArrayList<String>();
        while(rs.next()){
            columns.add(rs.getString("column_name"));
        }
        if(connection != null) connection.close();
        if(statement != null) statement.close();
        if(rs != null) rs.close();
        return columns;
    }

//    @Override
//    public List<String> matchDatabases(String database) throws Exception {
//        Connection connection = getConnection();
//        Statement statement = connection.createStatement();
//        String sql = String.format("select name from DBS where name like '%s%'", database);
//        ResultSet rs = statement.executeQuery(sql);
//        List<String> databases = new ArrayList<>();
//        while(rs.next()){
//            databases.add(rs.getString("name"));
//        }
//        if(connection != null) connection.close();
//        if(statement != null) statement.close();
//        if(rs != null) rs.close();
//        return databases;
//    }
//
//    @Override
//    public List<String> matchTables(String database, String table) throws Exception {
//        Connection connection = getConnection();
//        Statement statement = connection.createStatement();
//        String sql_format = "select t.tbl_name from TBLS t, DBS d where t.db_id=d.db_id and d.name='%s' and t.tbl_name like '%s%'";
//        String sql = String.format(sql_format, database,table);
//        ResultSet rs = statement.executeQuery(sql);
//        List<String> tables = new ArrayList<String>();
//        while(rs.next()){
//            tables.add(rs.getString("tbl_name"));
//        }
//        if(connection != null) connection.close();
//        if(statement != null) statement.close();
//        if(rs != null) rs.close();
//        return tables;
//    }
//
//    @Override
//    public List<String> matchColumns(String database, String table, String column) throws Exception {
//        Connection connection = getConnection();
//        Statement statement = connection.createStatement();
//        String sql_format =
//                "select c.column_name " +
//                        "from DBS d join TBLS t on d.db_id=t.db_id " +
//                        "join SDS s on t.sd_id=s.sd_id " +
//                        "join COLUMNS_V2 c on s.cd_id=c.cd_id " +
//                        "where d.name='%s' and t.tbl_name='%s' and c.column_name like '%s%';";
//        String sql = String.format(sql_format, database, table,column);
//        ResultSet rs = statement.executeQuery(sql);
//        List<String> columns = new ArrayList<>();
//        while(rs.next()){
//            columns.add(rs.getString("column_name"));
//        }
//        if(connection != null) connection.close();
//        if(statement != null) statement.close();
//        if(rs != null) rs.close();
//        return columns;
//    }
}
