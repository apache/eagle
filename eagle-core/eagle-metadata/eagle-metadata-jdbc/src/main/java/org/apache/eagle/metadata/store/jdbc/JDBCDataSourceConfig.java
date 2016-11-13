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

/**
 * Configuration:
 *
 * <p>Prefix: <code>metadata.jdbc.*</code>
 *
 * <p><code>
 * metadata {
 * jdbc {
 * username = ""
 * password = ""
 * driverClassName = "org.h2.Driver"
 * url = "jdbc:h2:./eagle"
 * connectionProperties = "encoding=UTF8"
 * }
 * }
 * </code>
 *
 * <p>https://commons.apache.org/proper/commons-dbcp/configuration.html
 */
public class JDBCDataSourceConfig {
    public static final String CONFIG_PREFIX = "metadata.jdbc";
    private static final String DEFAULT_DRIVER_CLASS = org.h2.Driver.class.getName();
    private static final String DEFAULT_CONNECTION_PROPERTIES = "encoding=UTF8";
    private static final String DEFAULT_URL = "jdbc:h2:./eagle";

    private String username;
    private String password;
    private String driverClassName = DEFAULT_DRIVER_CLASS;
    private String url = DEFAULT_URL;
    private String database;
    private String connectionProperties = DEFAULT_CONNECTION_PROPERTIES;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(String connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    @Override
    public String toString() {
        return String.format("%s { \n driverClassName=%s \n url=%s \n database=%s \n connectionProperties=%s \n username=%s \n password=*****\n}",
                CONFIG_PREFIX,driverClassName,url, database, connectionProperties, username);
    }
}