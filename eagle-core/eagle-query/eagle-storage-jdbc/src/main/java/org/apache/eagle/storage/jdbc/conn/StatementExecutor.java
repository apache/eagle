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

/**
 * An interface for abstract StatementExecutor from detailed implementation
 *
 * @since 3/29/15
 */
public interface StatementExecutor {
    /**
     * Initialize statement for action like SELECT
     *
     * @param config
     */
    void init(ConnectionConfig config);

    /**
     * Initialize statement for action like CREATE, UPDATE, DELETE which need table name
     *
     * @param config connection config
     * @param tableName
     */
    void init(ConnectionConfig config,String tableName);
    /**
     * @return customized primary key builder if no built-in primary key generation method like auto-increment and so on.
     */
    PrimaryKeyBuilder<String> getPrimaryKeyBuilder();
}