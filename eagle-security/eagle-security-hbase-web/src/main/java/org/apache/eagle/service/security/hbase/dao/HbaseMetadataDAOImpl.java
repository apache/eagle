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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.service.security.hbase.dao;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseMetadataDAOImpl {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseMetadataDAOImpl.class);

    private Configuration hBaseConfiguration;
    private HbaseMetadataAccessConfig config;

    public HbaseMetadataDAOImpl(HbaseMetadataAccessConfig config) {
        this.config = config;
        this.hBaseConfiguration = HBaseConfiguration.create();
        this.hBaseConfiguration.set("hbase.zookeeper.quorum", this.config.getZkQuorum());
        this.hBaseConfiguration.set("hbase.zookeeper.property.clientPort", this.config.getZkClientPort());
    }

    private HBaseAdmin getHBaseAdmin() throws IOException {
        return new HBaseAdmin(this.hBaseConfiguration);
    }

    public List<String> getNamespaces() throws IOException {
        List ret = new ArrayList();
        HBaseAdmin admin = this.getHBaseAdmin();
        NamespaceDescriptor [] ns = admin.listNamespaceDescriptors();
        for(NamespaceDescriptor n : ns) {
            ret.add(n.getName());
        }
        closeHbaseConnection(admin);
        return ret;
    }

    public List<String> getTables(String namespace) throws IOException {
        List ret = new ArrayList();
        HBaseAdmin admin = this.getHBaseAdmin();
        TableName[] tables = admin.listTableNamesByNamespace(namespace);

        for(TableName tableName : tables) {
            ret.add(tableName.getQualifierAsString());
        }
        closeHbaseConnection(admin);
        return ret;
    }

    public List<String> getColumnFamilies(String tableName) throws IOException {
        List ret = new ArrayList();
        HBaseAdmin admin = this.getHBaseAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName.getBytes());

        HColumnDescriptor [] cfs = tableDescriptor.getColumnFamilies();
        for(HColumnDescriptor cf : cfs) {
            ret.add(cf.getNameAsString());
        }
        closeHbaseConnection(admin);
        return ret;
    }

    private void closeHbaseConnection(HBaseAdmin admin) {
        if(admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
