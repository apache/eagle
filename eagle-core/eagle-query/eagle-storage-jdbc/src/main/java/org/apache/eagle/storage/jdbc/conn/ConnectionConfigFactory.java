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

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.storage.jdbc.JdbcConstants;


public class ConnectionConfigFactory {
    /**
     * Read connection config from config.properties.
     *
     * @return
     */
    public static ConnectionConfig getFromEagleConfig() {
        String username = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_DB_USERNAME);
        String password = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_DB_PASSWORD);
        String connUrl = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_CONN_URL);

        ConnectionConfig config = new ConnectionConfig();
        if (username != null) {
            config.setUserName(username);
        }
        if (password != null) {
            config.setPassword(password);
        }
        if (connUrl != null) {
            config.setConnectionUrl(connUrl);
        }
        String connProps = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_CONN_PROPS);
        if (connProps != null) {
            config.setConnectionProperties(connProps);
        }
        String adapter = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_ADAPTER);
        if (adapter != null) {
            config.setAdapter(adapter);
        }
        String databaseName = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_DATABASE);
        if (databaseName != null) {
            config.setDatabaseName(databaseName);
        }
        String driverClass = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_DRIVER_CLASS);
        if (driverClass != null) {
            config.setDriverClassName(driverClass);
        }
        String connMaxSize = EagleConfigFactory.load().getConfig().getString(JdbcConstants.EAGLE_CONN_MAX_SIZE);
        if (connMaxSize != null) {
            config.setConnectionMaxActive(Integer.parseInt(connMaxSize));
        }

        return config;
    }
}
