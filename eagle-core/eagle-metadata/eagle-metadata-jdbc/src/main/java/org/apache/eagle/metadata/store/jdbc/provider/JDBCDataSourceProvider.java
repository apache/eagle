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
package org.apache.eagle.metadata.store.jdbc.provider;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.eagle.metadata.store.jdbc.JDBCDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

public class JDBCDataSourceProvider implements Provider<DataSource> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCDataSourceProvider.class);

    @Inject
    private JDBCDataSourceConfig config;

    @Override
    public DataSource get() {
        BasicDataSource datasource = new BasicDataSource();
        datasource.setDriverClassName(config.getDriverClassName());
        datasource.setUsername(config.getUsername());
        datasource.setPassword(config.getPassword());
        datasource.setUrl(config.getUrl());
        datasource.setConnectionProperties(config.getConnectionProperties());
        LOGGER.info("Register JDBCDataSourceShutdownHook");
        Runtime.getRuntime().addShutdownHook(new Thread("JDBCDataSourceShutdownHook") {
            @Override
            public void run() {
                try {
                    LOGGER.info("Shutting down data source");
                    datasource.close();
                } catch (SQLException e) {
                    LOGGER.error("SQLException: {}", e.getMessage(), e);
                    throw new IllegalStateException("Failed to close datasource", e);
                }
            }
        });
        return datasource;
    }
}
