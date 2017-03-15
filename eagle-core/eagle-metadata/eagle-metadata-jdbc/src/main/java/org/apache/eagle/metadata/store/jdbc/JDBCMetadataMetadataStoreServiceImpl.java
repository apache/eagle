/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.store.jdbc;


import com.google.inject.Inject;
import org.apache.eagle.common.function.ThrowableConsumer;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


public class JDBCMetadataMetadataStoreServiceImpl implements JDBCMetadataQueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCMetadataMetadataStoreServiceImpl.class);

    @Inject
    private DataSource dataSource;

    @Override
    public boolean execute(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            return statement.execute(sql);
        } catch (SQLException e) {
            throw e;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public boolean dropTable(String tableName) throws SQLException {
        LOGGER.debug("Dropping table {}", tableName);
        return execute(String.format("DROP TABLE %s", tableName));
    }

    @Override
    public <T, E extends Throwable> int insert(String insertSql, Collection<T> entities, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws E, SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(insertSql);
            connection.setAutoCommit(false);
            for (T entity : entities) {
                mapper.accept(statement, entity);
                statement.addBatch();
            }
            int[] num = statement.executeBatch();
            connection.commit();
            int sum = 0;
            for (int i : num) {
                sum += i;
            }
            return sum;
        } catch (SQLException ex) {
            LOGGER.error("Error to insert batch: {}", insertSql, ex);
            throw ex;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public boolean forceDropTable(String tableName) {
        try {
            return dropTable(tableName);
        } catch (SQLException e) {
            LOGGER.debug(e.getMessage(), e);
        }
        return true;
    }

    @Override
    public <T, E extends Throwable> List<T> query(String sqlQuery, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(sqlQuery);
            resultSet = statement.executeQuery();
            List<T> result = new LinkedList<>();
            while (resultSet.next()) {
                result.add(mapper.apply(resultSet));
            }
            return result;
        } catch (SQLException e) {
            LOGGER.error("Error to query batch: {}", sqlQuery, e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public <T, E extends Throwable> List<T> queryWithCond(String sqlQuery, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper1, ThrowableFunction<ResultSet, T, E> mapper) throws
        SQLException, E {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(sqlQuery);
            mapper1.accept(statement, entity);
            resultSet = statement.executeQuery();
            List<T> result = new LinkedList<>();
            while (resultSet.next()) {
                result.add(mapper.apply(resultSet));
            }
            return result;
        } catch (SQLException e) {
            LOGGER.error("Error to query cond: {}", sqlQuery, e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public <T, E extends Throwable> List<T> queryWithCond(String querySql,
                                                          ThrowableConsumer<PreparedStatement, SQLException> preparer,
                                                          ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(querySql);
            preparer.accept(statement);
            resultSet = statement.executeQuery();
            List<T> result = new LinkedList<>();
            while (resultSet.next()) {
                result.add(mapper.apply(resultSet));
            }
            return result;
        } catch (SQLException e) {
            LOGGER.error("Error to query cond: {}", querySql, e);
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public <T, E extends Throwable> int update(String updateSql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws SQLException, E {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(updateSql);
            mapper.accept(statement, entity);
            return statement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("Error to update: {}", updateSql, e);
            throw e;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }
}