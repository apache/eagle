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

import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MetadataUtils {

    private final static Logger LOG = LoggerFactory.getLogger(MetadataUtils.class);

    public static <T> String getKey(T t) {
        if (t instanceof StreamDefinition) {
            return ((StreamDefinition) t).getStreamId();
        }
        if (t instanceof PublishmentType) {
            return ((PublishmentType) t).getType();
        }
        if (t instanceof PolicyAssignment) {
            return ((PolicyAssignment) t).getPolicyName();
        }
        if (t instanceof ScheduleState) {
            return ((ScheduleState) t).getVersion();
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
        String conn = config.getString("connection");
        try {
            connection = DriverManager.getConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
