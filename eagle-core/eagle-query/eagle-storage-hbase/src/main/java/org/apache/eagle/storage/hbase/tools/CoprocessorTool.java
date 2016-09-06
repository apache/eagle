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

public class CoprocessorTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoprocessorTool.class);


    public static void main(String[] args) throws IOException {
        if(args.length < 2){
            System.err.println("Usage: java "+CoprocessorTool.class.getName()+" [register/unregister] [hbase table name] [coprocessor jar path]");
            System.exit(1);
        }
        String command = args[0];
        String tableName = args[1];

        switch (command.toLowerCase()){
            case "register":
                if(args.length < 3){
                    System.err.println("Error: coprocessor jar path is missing");
                    System.err.println("Usage: java "+CoprocessorTool.class.getName()+" register "+tableName+" [coprocessor jar path]");
                    System.exit(1);
                }
                String jarPath = args[2];
                registerCoprocessor(jarPath,tableName);
                break;
            case "unregister":
                unregisterCoprocessor(tableName);
                break;
            default:
                System.err.println("Unsupported command type: " + command);
                System.err.println("Usage: java "+CoprocessorTool.class.getName()+" [register/unregister] [hbase table name] [coprocessor jar path]");
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
                throw new IOException("No coprocessor was registered on table "+tableName);
            } else {
                tableDescriptor.removeCoprocessor(AggregateProtocolEndPoint.class.getName());
                admin.modifyTable(table, tableDescriptor);
                System.out.println("Succeed to remove coprocessor from table " + tableName);
            }
        }
    }

    private static void registerCoprocessor(String jarPath,String tableName) throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration); HBaseAdmin admin = new HBaseAdmin(configuration)) {
            Path path = new Path(fs.getUri() + Path.SEPARATOR + jarPath);
            LOGGER.info("Checking path {} ... " + path.toString());
            if (!fs.exists(path)) {
               throw new IOException("Coprocessor jar: " + path.toString() + " not exists, please upload jar to hdfs firstly");
            } else {
                LOGGER.info("Path {} found", path.toString());
            }
            LOGGER.info("Checking hbase table {}", tableName);
            TableName table = TableName.valueOf(tableName);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(table);
            LOGGER.info("Table {} found", tableName);
            if (!tableDescriptor.hasCoprocessor(AggregateProtocolEndPoint.class.getName())) {
                tableDescriptor.addCoprocessor(AggregateProtocolEndPoint.class.getName(),
                        path,Coprocessor.PRIORITY_USER, new HashMap<>());
                admin.modifyTable(table, tableDescriptor);
                admin.close();
                System.out.println("Succeed to enable coprocessor on table " + tableName);
            } else {
                throw new IOException("Table '" + tableName + "' already registered coprocessor: " + AggregateProtocolEndPoint.class.getName());
            }
        }
    }
}
