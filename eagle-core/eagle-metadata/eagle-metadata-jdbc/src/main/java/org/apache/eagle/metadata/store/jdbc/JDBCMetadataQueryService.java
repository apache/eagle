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

import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public interface JDBCMetadataQueryService {
    /**
     * @param sql
     * @return
     * @throws SQLException
     */
    boolean execute(String sql) throws SQLException;

    /**
     * @param tableName
     * @return
     * @throws SQLException
     */
    boolean dropTable(String tableName) throws SQLException;

    /**
     * @param insertSql
     * @param entities
     * @param mapper
     * @param <T>
     * @param <E>
     * @return
     * @throws E
     * @throws SQLException
     */
    <T, E extends Throwable> int insert(String insertSql, Collection<T> entities, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws E, SQLException;

    /**
     * @param tableName
     * @return
     * @throws SQLException
     */
    boolean forceDropTable(String tableName);

    /**
     * @param querySql sql query text
     * @param mapper   result set to entity mapper
     * @param <T>
     * @return entity list
     */
    <T, E extends Throwable> List<T> query(String querySql, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E;

    /**
     * @param querySql sql query text
     * @param entity   query condition
     * @param mapper1
     * @param mapper   result set to entity mapper
     * @param <T>
     * @return entity list
     */
    <T, E extends Throwable> List<T> queryWithCond(String querySql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper1, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E;

    /**
     * @param updateSql update query text
     * @param entity   update condition
     * @param mapper
     * @param <T>
     * @return entity list
     */
    <T, E extends Throwable> int update(String updateSql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws SQLException, E;

}