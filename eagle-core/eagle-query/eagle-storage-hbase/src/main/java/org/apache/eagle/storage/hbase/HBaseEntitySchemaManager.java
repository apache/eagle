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

package org.apache.eagle.storage.hbase;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HBaseEntitySchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseEntitySchemaManager.class);
    private static HBaseEntitySchemaManager instance;
    private volatile HBaseAdmin admin;

    private static final int DEFAULT_MAX_VERSIONS = 1;

    private HBaseEntitySchemaManager() {
    }

    public static HBaseEntitySchemaManager getInstance() {
        if (instance == null) {
            synchronized (HBaseEntitySchemaManager.class) {
                if (instance == null) {
                    instance = new HBaseEntitySchemaManager();
                }
            }
        }
        return instance;
    }

    public void init() {
        if (!EagleConfigFactory.load().isAutoCreateTable()) {
            LOG.debug("Auto create table disabled, skip creating table");
            return;
        }
        Configuration conf = EagleConfigFactory.load().getHBaseConf();

        try {
            admin = new HBaseAdmin(conf);
            Map<String, EntityDefinition> entityServiceMap = EntityDefinitionManager.entities();
            if (entityServiceMap != null) {
                for (EntityDefinition entityDefinition : entityServiceMap.values()) {
                    createTable(entityDefinition);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void createTable(EntityDefinition entityDefinition) throws IOException {
        String tableName = entityDefinition.getTable();
        if (admin.tableExists(tableName)) {
            LOG.info("Table {} already exists", tableName);
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            // Adding column families to table descriptor
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(entityDefinition.getColumnFamily());
            columnDescriptor.setBloomFilterType(BloomType.ROW);
            //columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
            columnDescriptor.setMaxVersions(DEFAULT_MAX_VERSIONS);

            tableDescriptor.addFamily(columnDescriptor);

            // Execute the table through admin
            admin.createTable(tableDescriptor);
            LOG.info("Successfully create Table {}", tableName);
        }
    }
}

