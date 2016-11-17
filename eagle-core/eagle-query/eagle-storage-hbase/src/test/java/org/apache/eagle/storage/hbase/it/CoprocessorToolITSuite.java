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
package org.apache.eagle.storage.hbase.it;

import org.apache.eagle.storage.hbase.tools.CoprocessorTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Ignore("Coprocessor CLI Tool Integration Test.")
public class CoprocessorToolITSuite {
    private static final String remoteJarPath = "/tmp/eagle-storage-hbase-latest-coprocessor.jar";
    private static String localJarPath = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(CoprocessorToolITSuite.class);
    private static final String toolITTableName = "coprocessor_it_table";

    static {
        Configuration.addDefaultResource("hbase-site-sandbox.xml");
        localJarPath = CoprocessorJarUtils.getCoprocessorJarFile().getPath();
    }

    private void testRegisterCoprocessor(String tableName) throws Exception {
        CoprocessorTool.main(new String[] {
            "--register",
            "--config", "hbase-site-sandbox.xml",
            "--table", tableName,
            "--jar", remoteJarPath,
            "--localJar", localJarPath});
    }

    private void testUnregisterCoprocessor(String tableName) throws Exception {
        CoprocessorTool.main(new String[] {
            "--unregister",
            "--config", "hbase-site-sandbox.xml",
            "--table", tableName
        });
    }

    private void ensureTable() throws IOException {
        LOGGER.info("Creating table {}", toolITTableName);
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(toolITTableName));
        hTableDescriptor.addFamily(new HColumnDescriptor("f"));
        admin.createTable(hTableDescriptor);
        admin.close();
        LOGGER.info("Created table {}", toolITTableName);
    }

    @Test
    public void testRegisterAndUnregisterCoprocessor() throws Exception {
        try {
            ensureTable();
            testRegisterCoprocessor(toolITTableName);
            testUnregisterCoprocessor(toolITTableName);
        } finally {
            deleteTable();
        }
    }

    private void deleteTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        admin.disableTable(TableName.valueOf(toolITTableName));
        admin.deleteTable(TableName.valueOf(toolITTableName));
        admin.close();
    }

    @Test
    public void testRegisterCoprocessor() throws Exception {
        testRegisterCoprocessor("unittest");
    }
}