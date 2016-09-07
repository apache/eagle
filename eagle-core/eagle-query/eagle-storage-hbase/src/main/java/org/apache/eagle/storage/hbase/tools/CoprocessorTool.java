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
package org.apache.eagle.storage.hbase.tools;

import org.apache.eagle.storage.hbase.query.coprocessor.AggregateProtocolEndPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Coprocessor CLI Tool
 *
 * <code>
 * Usage: java org.apache.eagle.storage.hbase.tools.CoprocessorTool [enable/disable] [tableName] [jarOnHdfs] [jarOnLocal]
 * </code>
 */
public class CoprocessorTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoprocessorTool.class);

    private static void printUsage() {
        System.err.println("Usage: java " + CoprocessorTool.class.getName() + " [enable/disable] [tableName] [jarOnHdfs] [jarOnLocal]");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }
        String command = args[0];
        String tableName = args[1];

        switch (command.toLowerCase()) {
            case "enable":
                if (args.length < 3) {
                    System.err.println("Error: coprocessor jar path is missing");
                    System.err.println("Usage: java " + CoprocessorTool.class.getName() + " enable " + tableName + " [jarOnHdfs] [jarOnLocal]");
                    System.exit(1);
                }
                String jarPath = args[2];
                LOGGER.info("Table name: {}", tableName);
                LOGGER.info("Coprocessor jar on hdfs: {}", jarPath);
                String localJarPath = null;
                if (args.length > 3) {
                    localJarPath = args[3];
                    LOGGER.info("Coprocessor jar on local: {}", localJarPath);
                }
                String[] tableNames = tableName.split(",\\s*");
                for (String table : tableNames) {
                    LOGGER.info("Registering coprocessor for table {}", table);
                    registerCoprocessor(jarPath, table, localJarPath);
                }
                break;
            case "disable":
                unregisterCoprocessor(tableName);
                break;
            default:
                System.err.println("Unsupported command type: " + command);
                printUsage();
                System.exit(1);
        }
    }

    private static void unregisterCoprocessor(String tableName) throws IOException {
        Configuration configuration = new Configuration();
        TableName table = TableName.valueOf(tableName);
        try (HBaseAdmin admin = new HBaseAdmin(configuration)) {
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(table);
            LOGGER.info("Table {} found", tableName);
            if (tableDescriptor.hasCoprocessor(AggregateProtocolEndPoint.class.getName())) {
                LOGGER.warn("No coprocessor was registered on table '{}'", tableName);
                throw new IOException("No coprocessor was registered on table " + tableName);
            } else {
                tableDescriptor.removeCoprocessor(AggregateProtocolEndPoint.class.getName());
                admin.modifyTable(table, tableDescriptor);
                LOGGER.info("Succeed to remove coprocessor from table " + tableName);
            }
        }
    }

    private static void registerCoprocessor(String jarPath, String tableName, String localJarPath) throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration); HBaseAdmin admin = new HBaseAdmin(configuration)) {
            Path path = new Path(fs.getUri() + Path.SEPARATOR + jarPath);
            LOGGER.info("Checking path {} ... ", path.toString());
            if (!fs.exists(path)) {
                LOGGER.info("Path: {} not exist, uploading jar ...", path.toString());
                if (localJarPath == null) {
                    throw new IOException("local jar path is not given, please manually upload coprocessor jar onto hdfs at " + jarPath
                            + " and retry, or provide local coprocessor jar path through CLI argument and upload automatically");
                }
                LOGGER.info("Copying from local {} to {}", localJarPath, jarPath);
                fs.copyFromLocalFile(new Path(localJarPath), path);
                LOGGER.info("Succeed to copied coprocessor jar to {}", path.toString());
            } else {
                LOGGER.info("Path {} already exists", path.toString());
            }
            LOGGER.info("Checking hbase table {}", tableName);
            TableName table = TableName.valueOf(tableName);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(table);
            LOGGER.info("Table {} found", tableName);
            if (tableDescriptor.hasCoprocessor(AggregateProtocolEndPoint.class.getName())) {
                LOGGER.warn("Table '" + tableName + "' already registered coprocessor: " + AggregateProtocolEndPoint.class.getName()+", removing firstly");
                tableDescriptor.removeCoprocessor(AggregateProtocolEndPoint.class.getName());
                admin.modifyTable(table, tableDescriptor);
                tableDescriptor = admin.getTableDescriptor(table);
            }
            tableDescriptor.addCoprocessor(AggregateProtocolEndPoint.class.getName(),
                    path, Coprocessor.PRIORITY_USER, new HashMap<>());
            admin.modifyTable(table, tableDescriptor);
            LOGGER.info("Succeed to enable coprocessor on table " + tableName);
        }
    }
}
