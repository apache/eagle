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

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.metadata.MetadataUtils;
import org.apache.eagle.alert.metadata.resource.OpResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class JdbcMetadataHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataHandler.class);
    // general model
    private static final String INSERT_STATEMENT = "INSERT INTO %s(content, id) VALUES (?, ?)";
    private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE id=?";
    private static final String UPDATE_STATEMENT = "UPDATE %s set content=? WHERE id=?";
    private static final String QUERY_ALL_STATEMENT = "SELECT content FROM %s";
    private static final String QUERY_CONDITION_STATEMENT = "SELECT content FROM %s WHERE id=?";
    private static final String QUERY_ORDERBY_STATEMENT = "SELECT content FROM %s ORDER BY id %s";

    // customized model
    private static final String CLEAR_SCHEDULESTATES_STATEMENT = "DELETE FROM schedule_state WHERE id NOT IN (SELECT id from (SELECT id FROM schedule_state ORDER BY id DESC limit ?) as states)";
    private static final String INSERT_ALERT_STATEMENT = "INSERT INTO alert_event(alertId, siteId, appIds, policyId, alertTimestamp, policyValue, alertData) VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String QUERY_ALERT_STATEMENT = "SELECT * FROM alert_event order by alertTimestamp DESC limit ?";
    private static final String QUERY_ALERT_BY_ID_STATEMENT = "SELECT * FROM alert_event WHERE alertId=? order by alertTimestamp DESC limit ?";
    private static final String QUERY_ALERT_BY_POLICY_STATEMENT = "SELECT * FROM alert_event WHERE policyId=? order by alertTimestamp DESC limit ?";
    private static final String INSERT_POLICYPUBLISHMENT_STATEMENT = "INSERT INTO policy_publishment(policyId, publishmentName) VALUES (?, ?)";
    private static final String DELETE_PUBLISHMENT_STATEMENT = "DELETE FROM policy_publishment WHERE policyId=?";
    private static final String QUERY_PUBLISHMENT_BY_POLICY_STATEMENT = "SELECT content FROM publishment a INNER JOIN policy_publishment b ON a.id=b.publishmentName and b.policyId=?";
    private static final String QUERY_PUBLISHMENT_STATEMENT = "SELECT a.content, b.policyId FROM publishment a LEFT JOIN policy_publishment b ON a.id=b.publishmentName";

    public enum SortType { DESC, ASC }

    private static Map<String, String> tblNameMap = new HashMap<>();

    private static final ObjectMapper mapper = new ObjectMapper();
    private DataSource dataSource;

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        registerTableName(StreamingCluster.class.getSimpleName(), "stream_cluster");
        registerTableName(StreamDefinition.class.getSimpleName(), "stream_definition");
        registerTableName(Kafka2TupleMetadata.class.getSimpleName(), "kafka_tuple_metadata");
        registerTableName(PolicyDefinition.class.getSimpleName(), "policy_definition");
        registerTableName(Publishment.class.getSimpleName(), "publishment");
        registerTableName(PublishmentType.class.getSimpleName(), "publishment_type");
        registerTableName(ScheduleState.class.getSimpleName(), "schedule_state");
        registerTableName(PolicyAssignment.class.getSimpleName(), "policy_assignment");
        registerTableName(Topology.class.getSimpleName(), "topology");
        registerTableName(AlertPublishEvent.class.getSimpleName(), "alert_event");
    }

    private static void registerTableName(String clzName, String tblName) {
        tblNameMap.put(clzName, tblName);
    }

    public JdbcMetadataHandler(Config config) {
        try {
            //JdbcSchemaManager.getInstance().init(config);
            BasicDataSource bDatasource = new BasicDataSource();
            bDatasource.setDriverClassName(config.getString(MetadataUtils.JDBC_DRIVER_PATH));
            if (config.hasPath(MetadataUtils.JDBC_USERNAME_PATH)) {
                bDatasource.setUsername(config.getString(MetadataUtils.JDBC_USERNAME_PATH));
                bDatasource.setPassword(config.getString(MetadataUtils.JDBC_PASSWORD_PATH));
            }
            bDatasource.setUrl(config.getString(MetadataUtils.JDBC_CONNECTION_PATH));
            if (config.hasPath(MetadataUtils.JDBC_CONNECTION_PROPERTIES_PATH)) {
                bDatasource.setConnectionProperties(config.getString(MetadataUtils.JDBC_CONNECTION_PROPERTIES_PATH));
            }
            this.dataSource = bDatasource;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
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

    private void closeResource(ResultSet rs, PreparedStatement statement, Connection connection) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.info(e.getMessage(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Failed to close statement: {}", e.getMessage(), e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Failed to close connection: {}", e.getMessage(), e.getCause());
            }
        }
    }

    private OpResult executeUpdate(Connection connection, String query, String key, String value) throws SQLException {
        OpResult result = new OpResult();
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(query);
            Clob clob = connection.createClob();
            clob.setString(1, value);
            statement.setClob(1, clob);
            statement.setString(2, key);
            int status = statement.executeUpdate();
            LOG.info("update {} with query={}", status, query);
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        return result;
    }


    public <T> OpResult addOrReplace(String clzName, T t) {
        String tb = getTableName(clzName);
        OpResult result = new OpResult();
        Savepoint savepoint = null;
        String key = null;
        String value = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            key = MetadataUtils.getKey(t);
            value = mapper.writeValueAsString(t);
            connection.setAutoCommit(false);
            savepoint = connection.setSavepoint("insertEntity");
            result = executeUpdate(connection, String.format(INSERT_STATEMENT, tb), key, value);
            connection.commit();
        } catch (SQLException e) {
            LOG.warn("fail to insert entity due to {}, and try to updated instead", e.getMessage());
            if (connection != null) {
                LOG.info("Detected duplicated entity");
                try {
                    connection.rollback(savepoint);
                    executeUpdate(connection, String.format(UPDATE_STATEMENT, tb), key, value);
                    connection.commit();
                    connection.setAutoCommit(true);
                } catch (SQLException e1) {
                    LOG.warn("Rollback failed", e1);
                }
            }
        } catch (JsonProcessingException e) {
            LOG.error("Got JsonProcessingException: {}", e.getMessage(), e.getCause());
            result.code = OpResult.FAILURE;
            result.message = e.getMessage();
        } finally {
            closeResource(null, null, connection);
        }
        return result;
    }


    public <T> List<T> list(Class<T> clz) {
        return list(clz, null);
    }

    public <T> List<T> list(Class<T> clz,  SortType sortType) {
        List<T> result = new LinkedList<T>();
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            String tb = getTableName(clz.getSimpleName());
            String query = String.format(QUERY_ALL_STATEMENT, tb);
            if (sortType != null) {
                query = String.format(QUERY_ORDERBY_STATEMENT, tb, sortType.toString());
            }
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(query);
            return executeList(statement, clz);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            closeResource(null, statement, connection);
        }
        return result;
    }

    private <T> List<T> executeList(PreparedStatement statement, Class<T> clz) throws SQLException {
        List<T> result = new LinkedList<>();
        ResultSet rs = null;
        try {
            rs = statement.executeQuery();
            while (rs.next()) {
                try {
                    String content = rs.getString(1);
                    result.add(mapper.readValue(content, clz)) ;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        return result;
    }

    private <T> List<T> executeList(PreparedStatement statement, Function<ResultSet, T> selectFun) throws SQLException {
        List<T> result = new LinkedList<>();
        ResultSet rs = null;
        try {
            rs = statement.executeQuery();
            while (rs.next()) {
                result.add(selectFun.apply(rs));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        return result;
    }

    public <T> T queryById(Class<T> clz, String id) {
        List<T> result = new LinkedList<T>();
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            String tb = getTableName(clz.getSimpleName());
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(String.format(QUERY_CONDITION_STATEMENT, tb));
            statement.setString(1, id);
            result = executeList(statement, clz);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            closeResource(null, statement, connection);
        }
        if (result.isEmpty()) {
            return null;
        } else {
            return result.get(0);
        }
    }

    public AlertPublishEvent getAlertEventById(String alertId, int size) {
        List<AlertPublishEvent> alerts = listAlertEvents(QUERY_ALERT_BY_ID_STATEMENT, alertId, size);
        if (alerts.isEmpty()) {
            return null;
        } else {
            return alerts.get(0);
        }
    }

    public List<AlertPublishEvent> getAlertEventByPolicyId(String policyId, int size) {
        return listAlertEvents(QUERY_ALERT_BY_POLICY_STATEMENT, policyId, size);
    }

    public List<AlertPublishEvent> listAlertEvents(String query, String filter, int size) {
        List<AlertPublishEvent> alerts = new LinkedList<>();
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            if (query == null) {
                query = QUERY_ALERT_STATEMENT;
                statement = connection.prepareStatement(query);
                statement.setInt(1, size);
            } else {
                statement = connection.prepareStatement(query);
                statement.setString(1, filter);
                statement.setInt(2, size);
            }
            alerts = executeList(statement, rs -> {
                try {
                    AlertPublishEvent event = new AlertPublishEvent();
                    event.setAlertId(rs.getString(1));
                    event.setSiteId(rs.getString(2));
                    event.setAppIds(mapper.readValue(rs.getString(3), List.class));
                    event.setPolicyId(rs.getString(4));
                    event.setAlertTimestamp(rs.getLong(5));
                    event.setPolicyValue(rs.getString(6));
                    event.setAlertData(mapper.readValue(rs.getString(7), Map.class));
                    return event;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            closeResource(null, statement, connection);
        }
        return alerts;
    }

    public List<Publishment> listPublishments() {
        List<Publishment> result = new LinkedList<>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(QUERY_PUBLISHMENT_STATEMENT);
            Map<String, List<String>> publishPolicyMap = new HashedMap();
            rs = statement.executeQuery();
            while (rs.next()) {
                String publishment = rs.getString(1);
                String policyId = rs.getString(2);
                List<String> policyIds = publishPolicyMap.get(publishment);
                if (policyIds == null) {
                    policyIds = new ArrayList<>();
                    publishPolicyMap.put(publishment, policyIds);
                }
                if (policyId != null) {
                    policyIds.add(policyId);
                }
            }
            for (Map.Entry<String, List<String>> entry : publishPolicyMap.entrySet()) {
                Publishment publishment = mapper.readValue(entry.getKey(), Publishment.class);
                publishment.setPolicyIds(entry.getValue());
                result.add(publishment);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            closeResource(rs, statement, connection);
        }
        return result;
    }

    public List<Publishment> getPublishmentsByPolicyId(String policyId) {
        List<Publishment> result = new LinkedList<>();
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(QUERY_PUBLISHMENT_BY_POLICY_STATEMENT);
            statement.setString(1, policyId);
            result = executeList(statement, Publishment.class);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            closeResource(null, statement, connection);
        }
        return result;
    }

    public OpResult addAlertEvent(AlertPublishEvent event) {
        Connection connection = null;
        PreparedStatement statement = null;
        OpResult result = new OpResult();
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(INSERT_ALERT_STATEMENT);
            statement.setString(1, event.getAlertId());
            statement.setString(2, event.getSiteId());
            statement.setString(3, mapper.writeValueAsString(event.getAppIds()));
            statement.setString(4, event.getPolicyId());
            statement.setLong(5, event.getAlertTimestamp());
            statement.setString(6, event.getPolicyValue());
            statement.setString(7, mapper.writeValueAsString(event.getAlertData()));
            LOG.info("start to add alert event");
            int status = statement.executeUpdate();
            result.code = OpResult.SUCCESS;
            result.message = String.format("add %d records into alert_event successfully", status);
        } catch (Exception ex) {
            result.code = OpResult.FAILURE;
            result.message = ex.getMessage();
        } finally {
            closeResource(null, statement, connection);
        }
        LOG.info(result.message);
        return result;
    }

    public OpResult addPublishmentsToPolicy(String policyId, List<String> publishmentIds) {
        OpResult result = new OpResult();
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(DELETE_PUBLISHMENT_STATEMENT);
            statement.setString(1, policyId);
            int status = statement.executeUpdate();
            LOG.info("delete {} records from policy_publishment", status);
            closeResource(null, statement, null);

            statement = connection.prepareStatement(INSERT_POLICYPUBLISHMENT_STATEMENT);
            for (String pub : publishmentIds) {
                statement.setString(1, policyId);
                statement.setString(2, pub);
                statement.addBatch();
            }
            int[] num = statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            int sum = 0;
            for (int i : num) {
                sum += i;
            }
            result.code = OpResult.SUCCESS;
            result.message = String.format("Add %d records into policy_publishment", sum);
        } catch (SQLException ex) {
            LOG.error("Error to add publishments to policy {}", policyId, ex);
            result.code = OpResult.FAILURE;
            result.message = ex.getMessage();
        } finally {
            closeResource(null, statement, connection);
        }
        LOG.info(result.message);
        return result;
    }

    public OpResult removeById(String clzName, String key) {
        Connection connection = null;
        PreparedStatement statement = null;
        OpResult result = new OpResult();
        try {
            String tb = getTableName(clzName);
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(String.format(DELETE_STATEMENT, tb));
            statement.setString(1, key);
            LOG.info("start to delete records from {} with id={}", tb, key);
            int status = statement.executeUpdate();
            result.code = OpResult.SUCCESS;
            result.message = String.format("removed %d records from %s successfully", status, tb);
        } catch (SQLException ex) {
            result.code = OpResult.FAILURE;
            result.message = ex.getMessage();
        } finally {
            closeResource(null, statement, connection);
        }
        LOG.info(result.message);
        return result;
    }

    public void close() throws IOException {
        //JdbcSchemaManager.getInstance().shutdown();
    }

    public OpResult removeScheduleStates(int capacity) {
        Connection connection = null;
        PreparedStatement statement = null;
        OpResult result = new OpResult();
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(CLEAR_SCHEDULESTATES_STATEMENT);
            statement.setInt(1, capacity);
            LOG.info("start to delete schedule states");
            int status = statement.executeUpdate();
            result.code = OpResult.SUCCESS;
            result.message = String.format("removed %d records from schedule_state successfully", status);
        } catch (SQLException ex) {
            result.code = OpResult.FAILURE;
            result.message = ex.getMessage();
        } finally {
            closeResource(null, statement, connection);
        }
        LOG.info(result.message);
        return result;
    }
}
