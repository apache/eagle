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

package org.apache.eagle.storage.hbase;

import org.apache.eagle.storage.hbase.query.coprocessor.AggregateProtocolEndPoint;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestWithHBaseCoprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(TestWithHBaseCoprocessor.class);
    protected static HBaseTestingUtility hbase;

    protected static String getZkZnodeParent() {
        return "/hbase-test";
    }

    @BeforeClass
    public static void setUpHBase() throws IOException {
        System.setProperty("config.resource", "/application-co.conf");
        Configuration conf = HBaseConfiguration.create();
        conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AggregateProtocolEndPoint.class.getName());
        conf.set("zookeeper.znode.parent", getZkZnodeParent());
        conf.setInt("hbase.master.info.port", -1);//avoid port clobbering
        conf.setInt("hbase.regionserver.info.port", -1);//avoid port clobbering

        int attempts = 0;
        hbase = new HBaseTestingUtility(conf);
        boolean successToStart = false;
        while (attempts < 3) {
            try {
                attempts ++;
                hbase.startMiniCluster();
                successToStart = true;
            } catch (Exception e) {
                LOG.error("Error to start mini cluster (tried {} times): {}", attempts, e.getMessage(), e);
                try {
                    hbase.shutdownMiniCluster();
                } catch (Exception e1) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }

        Assert.assertTrue("Failed to start mini cluster in " + attempts + " attempts", successToStart);

        Table table = hbase.createTable(TableName.valueOf("unittest"),"f");
        HTableDescriptor descriptor = new HTableDescriptor(table.getTableDescriptor());
        descriptor.addCoprocessor(AggregateProtocolEndPoint.class.getName());
        hbase.getHBaseAdmin().modifyTable("unittest",descriptor);

        System.setProperty("storage.hbase.autoCreateTable","false");
        System.setProperty("storage.hbase.coprocessorEnabled", String.valueOf(true));
        System.setProperty("storage.hbase.zookeeperZnodeParent", getZkZnodeParent());
        System.setProperty("storage.hbase.zookeeperPropertyClientPort", String.valueOf(hbase.getZkCluster().getClientPort()));
    }

    @AfterClass
    public static void shutdownHBase() {
        try {
            hbase.shutdownMiniCluster();
        } catch (Exception e) {
            LOG.error("Error to shutdown mini cluster: " + e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }
}
