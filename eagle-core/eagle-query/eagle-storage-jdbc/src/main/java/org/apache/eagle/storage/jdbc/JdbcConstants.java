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
package org.apache.eagle.storage.jdbc;

import java.sql.Types;

/**
 * Jdbc Storage Constants
 */
public class JdbcConstants {
    // Eagle JDBC Schema
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String METRIC_NAME_COLUMN_NAME = "metric";
    public static final String ROW_KEY_COLUMN_NAME = "uuid";

    public static final int DEFAULT_TYPE_FOR_COMPLEX_TYPE = Types.BLOB;
    public static final int DEFAULT_VARCHAR_SIZE = 30000;

    // Eagle JDBC Storage Configuration
    public static final String EAGLE_DB_USERNAME = "storage.jdbc.username";
    public static final String EAGLE_DB_PASSWORD = "storage.jdbc.password";
    public static final String EAGLE_CONN_URL = "storage.jdbc.connectionUrl";
    public static final String EAGLE_CONN_PROPS = "storage.jdbc.connectionProps";
    public static final String EAGLE_ADAPTER = "storage.jdbc.adapter";
    public static final String EAGLE_DATABASE = "storage.jdbc.database";
    public static final String EAGLE_DRIVER_CLASS = "storage.jdbc.driverClass";
    public static final String EAGLE_CONN_MAX_SIZE = "storage.jdbc.connectionMax";

    public static final boolean isReservedField(String columnName) {
        return TIMESTAMP_COLUMN_NAME.equals(columnName) || METRIC_NAME_COLUMN_NAME.equals(columnName) || ROW_KEY_COLUMN_NAME.equals(columnName);
    }
}