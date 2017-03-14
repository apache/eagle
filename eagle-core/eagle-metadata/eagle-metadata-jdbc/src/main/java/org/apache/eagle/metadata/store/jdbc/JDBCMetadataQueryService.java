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

import org.apache.eagle.common.function.ThrowableConsumer;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface JDBCMetadataQueryService {
    boolean execute(String sql) throws SQLException;

    <T, E extends Throwable> boolean execute(String sql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws SQLException, E;

    boolean dropTable(String tableName) throws SQLException;

    <T, E extends Throwable> int insert(String insertSql, Collection<T> entities, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws E, SQLException;

    boolean forceDropTable(String tableName);

    <T, E extends Throwable> List<T> query(String querySql, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E;


    /**
     * @see #queryWithCond(String, ThrowableConsumer, ThrowableFunction)
     */
    <T, E extends Throwable> List<T> queryWithCond(String querySql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper1, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E;

    <T, E extends Throwable> List<T> queryWithCond(String querySql, ThrowableConsumer<PreparedStatement, SQLException> preparer, ThrowableFunction<ResultSet, T, E> mapper) throws SQLException, E;

    <T, E extends Throwable> int update(String updateSql, T entity, ThrowableConsumer2<PreparedStatement, T, E> mapper) throws SQLException, E;
}