/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.metadata.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.eagle.alert.metadata.MetadataUtils;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JdbcDatabaseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDatabaseHandler.class);

    private final String INSERT_STATEMENT = "INSERT INTO %s VALUES (?, ?)";
    private final String DELETE_STATEMENT = "DELETE FROM %s WHERE id=?";
    private final String UPDATE_STATEMENT = "UPDATE %s set value=? WHERE id=?";
    private final String QUERY_ALL_STATEMENT = "SELECT value FROM %s";
    private final String QUERY_CONDITION_STATEMENT = "SELECT value FROM %s WHERE id=?";
    private final String QUERY_ORDERBY_STATEMENT = "SELECT value FROM %s ORDER BY id %s";

    private Map<String, String> tblNameMap = new HashMap<>();

    private static final ObjectMapper mapper = new ObjectMapper();
    private Connection connection = null;

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public JdbcDatabaseHandler(Config config) {
        // "jdbc:mysql://dbhost/database?" + "user=sqluser&password=sqluserpw"
        this.tblNameMap = JdbcSchemaManager.tblNameMap;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            JdbcSchemaManager.getInstance().init(config);
            connection = MetadataUtils.getJdbcConnection(config);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String getTableName(String clzName) {
        String tbl = tblNameMap.get(clzName);
        if (tbl != null) {
            return tbl;
        } else {
            return clzName;
        }
    }

    public <T> OpResult addOrReplace(String clzName, T t) {
        String tb = getTableName(clzName);
        OpResult result = new OpResult();
        PreparedStatement statement = null;
        Savepoint savepoint = null;
        String key = null;
        String value = null;
        try {
            statement = connection.prepareStatement(String.format(INSERT_STATEMENT, tb));
            key = MetadataUtils.getKey(t);
            value = mapper.writeValueAsString(t);

            statement.setString(1, key);
            Clob clob = connection.createClob();
            clob.setString(1, value);
            statement.setClob(2, clob);

            connection.setAutoCommit(false);
            savepoint = connection.setSavepoint("insertEntity");
            int status = statement.executeUpdate();
            LOG.info("update {} entities", status);
            connection.commit();
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e.getCause());
            if (e.getMessage().toLowerCase().contains("duplicate")) {
                LOG.info("Detected duplicated entity");
                try {
                    connection.rollback(savepoint);
                    update(tb, key, value);
                } catch (SQLException e1) {
                    //e1.printStackTrace();
                    LOG.warn("Rollback failed", e1);
                }
            }
        } catch (JsonProcessingException e) {
            LOG.error("Got JsonProcessingException: {}", e.getMessage(), e.getCause());
            result.code = OpResult.FAILURE;
            result.message = e.getMessage();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.error("Failed to close statement: {}", e.getMessage(), e.getCause());
                }
            }
        }
        return result;
    }

    private <T> OpResult update(String tb, String key, String value) throws SQLException {
        OpResult result = new OpResult();
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(String.format(UPDATE_STATEMENT, tb));
            Clob clob = connection.createClob();
            clob.setString(1, value);
            statement.setClob(1, clob);
            statement.setString(2, key);

            int status = statement.executeUpdate();
            LOG.info("update {} entities from table {}", status, tb);
        } catch (SQLException e) {
            e.printStackTrace();
            result.code = OpResult.FAILURE;
            result.message = e.getMessage();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        return result;
    }

    public <T> List<T> list(Class<T> clz) {
        String tb = getTableName(clz.getSimpleName());
        List<T> result = new LinkedList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(QUERY_ALL_STATEMENT, tb));
            while (rs.next()) {
                //String key = rs.getString(1);
                String json = rs.getString(1);
                try {
                    T obj = mapper.readValue(json, clz);
                    result.add(obj);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public <T> T listWithFilter(String key, Class<T> clz) {
        String tb = getTableName(clz.getSimpleName());
        List<T> result = new LinkedList<>();
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(String.format(QUERY_CONDITION_STATEMENT, tb));
            statement.setString(1, key);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                //String key = rs.getString(1);
                String json = rs.getString(1);
                try {
                    T obj = mapper.readValue(json, clz);
                    result.add(obj);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("Close statement failed");
                }
            }
        }
        if (result.isEmpty()) {
            return null;
        } else {
            return result.get(0);
        }
    }

    public <T> T listTop(Class<T> clz, String sortType) {
        String tb = getTableName(clz.getSimpleName());
        List<T> result = new LinkedList<>();
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(String.format(QUERY_ORDERBY_STATEMENT, tb, sortType));
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                //String key = rs.getString(1);
                String json = rs.getString(1);
                try {
                    T obj = mapper.readValue(json, clz);
                    result.add(obj);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("Close statement failed");
                }
            }
        }
        if (result.isEmpty()) {
            return null;
        } else {
            return result.get(0);
        }
    }

    public <T> OpResult remove(String clzName, String key) {
        String tb = getTableName(clzName);
        OpResult result = new OpResult();
        try {
            PreparedStatement statement = connection.prepareStatement(String.format(DELETE_STATEMENT, tb, key));
            statement.setString(1, key);
            int status = statement.executeUpdate();
            String msg = String.format("delete %s entities from table %s", status, tb);
            result.code = OpResult.SUCCESS;
            result.message = msg;
            statement.close();
        } catch (SQLException e) {
            result.code = OpResult.FAILURE;
            result.message = e.getMessage();
            //e.printStackTrace();
        }
        return result;
    }

    public void close() throws IOException {
        //JdbcSchemaManager.getInstance().shutdown();
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
        }
    }

}
