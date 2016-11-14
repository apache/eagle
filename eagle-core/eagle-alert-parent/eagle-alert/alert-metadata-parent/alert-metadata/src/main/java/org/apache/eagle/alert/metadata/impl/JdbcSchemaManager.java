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

package org.apache.eagle.alert.metadata.impl;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.metadata.MetadataUtils;
import com.typesafe.config.Config;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JdbcSchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSchemaManager.class);
    private Database database;
    private Platform platform;

    private static JdbcSchemaManager instance;

    public static Map<String, String> tblNameMap = new HashMap<>();

    private JdbcSchemaManager() {
    }

    private static void registerTableName(String clzName, String tblName) {
        tblNameMap.put(clzName, tblName);
    }

    static {
        registerTableName(StreamingCluster.class.getSimpleName(), "cluster");
        registerTableName(StreamDefinition.class.getSimpleName(), "stream_schema");
        registerTableName(Kafka2TupleMetadata.class.getSimpleName(), "datasource");
        registerTableName(PolicyDefinition.class.getSimpleName(), "policy");
        registerTableName(Publishment.class.getSimpleName(), "publishment");
        registerTableName(PublishmentType.class.getSimpleName(), "publishment_type");
        registerTableName(ScheduleState.class.getSimpleName(), "schedule_state");
        registerTableName(PolicyAssignment.class.getSimpleName(), "assignment");
        registerTableName(Topology.class.getSimpleName(), "topology");
        registerTableName(AlertPublishEvent.class.getSimpleName(), "alert_event");
    }

    public static JdbcSchemaManager getInstance() {
        if (instance == null) {
            instance = new JdbcSchemaManager();
        }
        return instance;
    }

    public void init(Config config) {
        Connection connection = null;
        try {
            this.platform = PlatformFactory.createNewPlatformInstance("mysql");

            connection = MetadataUtils.getJdbcConnection(config);
            String dbName = config.getString(MetadataUtils.JDBC_DATABASE_PATH);
            this.database = platform.readModelFromDatabase(connection, dbName);
            LOG.info("Loaded " + database);

            Database _database = identifyNewTables();
            if (_database.getTableCount() > 0) {
                LOG.info("Creating {} new tables (totally {} tables)", _database.getTableCount(), database.getTableCount());
                this.platform.createTables(connection, _database, false, true);
                LOG.info("Created {} new tables: ", _database.getTableCount(), _database.getTables());
            } else {
                LOG.debug("All the {} tables have already been created, no new tables", database.getTableCount());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }
    }

    private Database identifyNewTables() {
        Database _database = new Database();
        _database.setName(database.getName());
        Collection<String> tableNames = tblNameMap.values();
        LOG.info("Initializing database and creating tables");
        for (String tableName : tableNames) {
            if (database.findTable(tableName) == null) {
                Table table = createTable(tableName);
                LOG.info("Creating {}", table.toVerboseString());
                _database.addTable(table);
                database.addTable(table);
            } else {
                LOG.debug("Table {} already exists", tableName);
            }
        }
        return _database;
    }

    public void shutdown() {
        this.platform.shutdownDatabase();
    }

    private Table createTable(String tableName) {
        Table table = new Table();
        table.setName(tableName);
        buildTable(table);
        return table;
    }

    private void buildTable(Table table) {
        Column id = new Column();
        id.setName("id");
        id.setPrimaryKey(true);
        id.setRequired(true);
        id.setTypeCode(Types.VARCHAR);
        id.setSize("50");
        table.addColumn(id);

        Column value = new Column();
        value.setName("value");
        value.setTypeCode(Types.CLOB);
        table.addColumn(value);
    }
}