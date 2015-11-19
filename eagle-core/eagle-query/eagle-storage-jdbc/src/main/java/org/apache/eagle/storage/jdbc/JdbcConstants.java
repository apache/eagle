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

/**
 * Jdbc Storage Constants
 */
public class JdbcConstants {
    // Eagle JDBC Schema
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String METRIC_NAME_COLUMN_NAME = "metric";
    public static final String ROW_KEY_COLUMN_NAME = "uuid";

    // Eagle JDBC Storage Configuration
    public final static String EAGLE_DB_USERNAME = "eagle.service.storage-username";
    public final static String EAGLE_DB_PASSWORD = "eagle.service.storage-password";
    public final static String EAGLE_CONN_URL= "eagle.service.storage-connection-url";
    public final static String EAGLE_CONN_PROPS= "eagle.service.storage-connection-props";
    public final static String EAGLE_ADAPTER= "eagle.service.storage-adapter";
    public final static String EAGLE_DATABASE= "eagle.service.storage-database";
    public final static String EAGLE_DRIVER_CLASS= "eagle.service.storage-driver-class";
    public final static String EAGLE_CONN_MAX_SIZE= "eagle.service.storage-connection-max";
}