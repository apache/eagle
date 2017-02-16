/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHBaseBase.class);
    protected static HBaseTestingUtility hbase;

    protected static String getZkZnodeParent() {
        return "/hbase-test";
    }

    @BeforeClass
    public static void setUpHBase() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("zookeeper.znode.parent", getZkZnodeParent());
        configuration.setInt("hbase.master.info.port", -1);//avoid port clobbering
        configuration.setInt("hbase.regionserver.info.port", -1);//avoid port clobbering
        hbase = new HBaseTestingUtility(configuration);
        try {
            hbase.startMiniCluster();
        } catch (Exception e) {
            LOGGER.error("Error to start hbase mini cluster: " + e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        System.setProperty("storage.hbase.autoCreateTable","false");
        System.setProperty("storage.hbase.zookeeperZnodeParent", getZkZnodeParent());
        System.setProperty("storage.hbase.zookeeperPropertyClientPort", String.valueOf(hbase.getZkCluster().getClientPort()));
    }

    @AfterClass
    public static void shutdownHBase() {
        try {
            hbase.shutdownMiniCluster();
        } catch (Exception e) {
            LOGGER.error("Error to shutdown mini hbase cluster: " + e.getMessage(),e);
        } finally {
            hbase = null;
        }
    }
}