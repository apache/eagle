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
package org.apache.eagle.storage.jdbc.conn;

import java.sql.Connection;

public interface ConnectionManager {
    /**
     * init.
     * @param config
     * @throws Exception
     */
    void init(ConnectionConfig config) throws Exception;

    ConnectionConfig getConfig();

    /**
     * getConnnetion.
     * @return
     * @throws Exception
     */
    Connection getConnection() throws Exception;

    /**
     * Get default database StatementExecutor.
     *
     * @return StatementExecutor implementation
     */
    <T extends StatementExecutor> T getStatementExecutor();

    /**
     * Get default database StatementExecutor.
     *
     * @return StatementExecutor implementation
     */
    <T extends StatementExecutor> T getStatementExecutor(String tableName);

    /**
     * close connection.
     * @param connection
     * @throws Exception
     */
    void close(Connection connection) throws Exception;

    /**
     * shutdown.
     * @throws Exception
     */
    void shutdown() throws Exception;

    boolean isClosed();
}