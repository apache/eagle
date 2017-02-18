/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class MetadataUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataUtils.class);
    public static final String META_DATA = "metadata";
    public static final String ALERT_META_DATA_DAO = "metadataDao";
    public static final String JDBC_USERNAME_PATH = "jdbc.username";
    public static final String JDBC_PASSWORD_PATH = "jdbc.password";
    public static final String JDBC_DRIVER_PATH = "jdbc.driverClassName";
    public static final String JDBC_DATABASE_PATH = "jdbc.database";
    public static final String JDBC_CONNECTION_PATH = "jdbc.connection";
    public static final String JDBC_CONNECTION_PROPERTIES_PATH = "jdbc.connectionProperties";
    public static final String MONGO_CONNECTION_PATH = "mongo.connection";
    public static final String MONGO_DATABASE = "mongo.database";

    public static <T> String getKey(T t) {
        if (t instanceof StreamDefinition) {
            return ((StreamDefinition) t).getStreamId();
        }
        if (t instanceof PolicyAssignment) {
            return ((PolicyAssignment) t).getPolicyName();
        }
        if (t instanceof ScheduleState) {
            return ((ScheduleState) t).getVersion();
        }
        if (t instanceof AlertPublishEvent) {
            return ((AlertPublishEvent) t).getAlertId();
        }

        try {
            Method m = t.getClass().getMethod("getName");
            return (String) m.invoke(t);
        } catch (NoSuchMethodException | SecurityException | InvocationTargetException | IllegalAccessException
            | IllegalArgumentException e) {
            LOG.error(" getName not found on given class :" + t.getClass().getName());
        }
        throw new RuntimeException(String.format("no getName() found on target class %s for matching", t.getClass()
            .getName()));
    }

    public static Connection getJdbcConnection(Config config) {

        Connection connection = null;
        try {
            if (config.hasPath(JDBC_USERNAME_PATH)) {
                connection = DriverManager.getConnection(
                        config.getString(JDBC_CONNECTION_PATH),
                        config.getString(JDBC_USERNAME_PATH),
                        config.getString(JDBC_PASSWORD_PATH));
            } else {
                connection = DriverManager.getConnection(config.getString(JDBC_CONNECTION_PATH));
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return connection;
    }
}
