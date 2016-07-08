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
package org.apache.eagle.alert.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

public class ZookeeperEmbedded {
    private TestingServer server;
    private CuratorFramework zookeeper;
    private int port;
    private File logDir;

    public ZookeeperEmbedded(int port) {
        this.port = port;
        this.logDir = new File(System.getProperty("java.io.tmpdir"), "zk/logs/zookeeper-test-" + port);
    }

    public void start() throws Exception {
        FileUtils.deleteQuietly(logDir);

        server = new TestingServer(this.port, this.logDir);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
        zookeeper.start();
    }

    public String getConnectionString() {
        return server.getConnectString();
    }

    public void shutdown() {
        try {
            if (!zookeeper.getState().equals(CuratorFrameworkState.STOPPED)) {
                zookeeper.close();
            }

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                FileUtils.deleteQuietly(logDir);
            }
        }
    }
}
