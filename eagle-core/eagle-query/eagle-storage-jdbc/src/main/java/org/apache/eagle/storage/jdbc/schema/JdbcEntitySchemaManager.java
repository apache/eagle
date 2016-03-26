package org.apache.eagle.storage.jdbc.schema;

/**
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

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.*;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.conn.ConnectionConfig;
import org.apache.eagle.storage.jdbc.conn.ConnectionConfigFactory;
import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;

public class JdbcEntitySchemaManager implements IJdbcEntityDDLManager {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntitySchemaManager.class);
    private Database database;
    private Platform platform;

    private static IJdbcEntityDDLManager instance;

    private JdbcEntitySchemaManager(){
        instance = null;
        ConnectionConfig config = ConnectionConfigFactory.getFromEagleConfig();
        this.platform = PlatformFactory.createNewPlatformInstance(config.getAdapter());
        Connection connection = null;
        try {
            connection = ConnectionManagerFactory.getInstance().getConnection();
            this.database = platform.readModelFromDatabase(connection,config.getDatabaseName());
            LOG.info("Loaded "+database);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(),e);
        } finally {
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn(e.getMessage(),e);
                }
            }
        }
    }

    public static IJdbcEntityDDLManager getInstance(){
        if(instance == null){
            instance = new JdbcEntitySchemaManager();
        }
        return instance;
    }

    @Override
    public void init() {
        Connection connection = null;
        try {
            Database _database = identifyNewTables();
            if(_database.getTableCount() >0 ) {
                LOG.info("Creating {} new tables (totally {} tables)", _database.getTableCount(),database.getTableCount());
                connection = ConnectionManagerFactory.getInstance().getConnection();
                this.platform.createTables(connection,_database, false, true);
                LOG.info("Created {} new tables: ",_database.getTableCount(),_database.getTables());
            } else {
                LOG.debug("All the {} tables have already been created, no new tables", database.getTableCount());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new IllegalStateException(e);
        } finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn(e.getMessage(),e);
                }
            }
        }
    }

    private Database identifyNewTables(){
        Database _database = new Database();
        _database.setName(database.getName());
        Collection<JdbcEntityDefinition> entityDefinitions = JdbcEntityDefinitionManager.getJdbcEntityDefinitionMap().values();
        LOG.info("Initializing database and creating tables");
        for (JdbcEntityDefinition entityDefinition : entityDefinitions) {
            if (database.findTable(entityDefinition.getJdbcTableName()) == null) {
                Table table = createTable(entityDefinition);
                LOG.info("Creating {}", table.toVerboseString());
                _database.addTable(table);
                database.addTable(table);
            } else {
                LOG.debug("Table {} already exists", entityDefinition.getJdbcTableName());
            }
        }
        return _database;
    }

    @Override
    public void reinit(){
        Connection connection = null;
        try {
            identifyNewTables();
            connection = ConnectionManagerFactory.getInstance().getConnection();
            this.platform.createTables(connection,database, true, true);
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new IllegalStateException(e);
        } finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.warn(e.getMessage(),e);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        this.platform.shutdownDatabase();
    }

    private Table createTable(JdbcEntityDefinition entityDefinition){
        Table table = new Table();
        table.setName(entityDefinition.getJdbcTableName());
        buildTable(entityDefinition,table);
        return table;
    }

    private Column createTagColumn(String tagName){
        Column tagColumn = new Column();
        tagColumn.setName(tagName);
        tagColumn.setTypeCode(Types.VARCHAR);
        tagColumn.setJavaName(tagName);
//        tagColumn.setScale(1024);
        tagColumn.setSize(String.valueOf(JdbcConstants.DEFAULT_VARCHAR_SIZE));
        tagColumn.setDefaultValue(null);
        tagColumn.setDescription("eagle entity tag column for "+tagName);
        return tagColumn;
    }

    private void buildTable(JdbcEntityDefinition entityDefinition, Table table){
        // METRIC
        if(entityDefinition.getInternal().getService()
                .equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
            Column metricColumn = new Column();
            metricColumn.setName(JdbcConstants.METRIC_NAME_COLUMN_NAME);
            metricColumn.setTypeCode(Types.VARCHAR);
//            metricColumn.setSizeAndScale(1024,1024);
            metricColumn.setDescription("eagle entity metric column");
            table.addColumn(metricColumn);
        }

        // ROWKEY
        Column pkColumn = new Column();
        pkColumn.setName(JdbcConstants.ROW_KEY_COLUMN_NAME);
        pkColumn.setPrimaryKey(true);
        pkColumn.setRequired(true);
        pkColumn.setTypeCode(Types.VARCHAR);
//        pkColumn.setSize("256");

        pkColumn.setDescription("eagle entity row-key column");
        table.addColumn(pkColumn);

        // TIMESTAMP
        Column tsColumn = new Column();
        tsColumn.setName(JdbcConstants.TIMESTAMP_COLUMN_NAME);
        tsColumn.setTypeCode(Types.BIGINT);
        tsColumn.setDescription("eagle entity timestamp column");
        table.addColumn(tsColumn);

        // TAGS
        if(entityDefinition.getInternal().getTags() != null) {
//            Index index = new UniqueIndex();
            for (String tag : entityDefinition.getInternal().getTags()) {
                Column tagColumn = createTagColumn(tag);
                tagColumn.setSize(String.valueOf(JdbcConstants.DEFAULT_VARCHAR_SIZE));
                table.addColumn(tagColumn);
//                IndexColumn indexColumn = new IndexColumn();
//                indexColumn.setName(tag);
//                indexColumn.setOrdinalPosition(0);
//                index.addColumn(indexColumn);
//                index.setName(entityDefinition.getJdbcTableName()+"_tags_unique_index");
            }
//            TODO: enable index when experiencing performance issue on tag filtering.
//            table.addIndex(index);
        }

        for(Map.Entry<String,Qualifier> entry: entityDefinition.getInternal().getDisplayNameMap().entrySet()){
            Column fieldColumn = new Column();
            fieldColumn.setName(entry.getKey());
            fieldColumn.setJavaName(entry.getKey());
            Integer typeCode = entityDefinition.getJdbcColumnTypeCodeOrNull(entry.getKey());
            typeCode = typeCode == null? Types.VARCHAR:typeCode;
            if(typeCode == Types.VARCHAR) fieldColumn.setSize(String.valueOf(JdbcConstants.DEFAULT_VARCHAR_SIZE));
            fieldColumn.setTypeCode(typeCode);
            fieldColumn.setDescription("eagle field column "+entry.getKey()+":"+entityDefinition.getColumnTypeOrNull(entry.getKey()));
            table.addColumn(fieldColumn);
        }
    }
}