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

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;

public class ZookeeperEmbedded {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperEmbedded.class);
    private static final int MAX_RETRIES = 3;
    private TestingServer server;
    private CuratorFramework zookeeper;
    private int port;
    private File logDir;

    /**
     * Create zookeeper testing server.
     *
     * @param port initial zookeeper port
     */
    public ZookeeperEmbedded(int port) {
        this.port = port;
        this.logDir = new File(System.getProperty("java.io.tmpdir"), "zk/logs/zookeeper-test-" + port);
    }

    /**
     * Try to start zookeeper, if failed, retry with <code>port+1</code>.
     *
     * @return finally bound port
     */
    public int start() throws Exception {
        FileUtils.deleteQuietly(logDir);

        int i = 0;
        boolean success = false;
        Exception lastException = null;
        while (!success && i < MAX_RETRIES) {
            try {
                server = new TestingServer(this.port, this.logDir);
                ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
                zookeeper = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
                zookeeper.start();
                success = true;
            } catch (BindException exception) {
                lastException = exception;
                i++;
                LOG.warn("Port {} was taken, trying {}", this.port, this.port + i);
                this.port = this.port + i;
                try {
                    server.close();
                    zookeeper.close();
                } catch (Throwable throwable) {
                    // ignored
                }
            }
        }
        if (!success) {
            LOG.error("Failed to start zookeeper after trying {} times", MAX_RETRIES);
            throw lastException;
        }
        return this.port;
    }

    public String getConnectionString() {
        return server.getConnectString();
    }

    public void shutdown() {
        try {
            if (zookeeper != null) {
                if (!zookeeper.getState().equals(CuratorFrameworkState.STOPPED)) {
                    zookeeper.close();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (server != null) {
                    server.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                FileUtils.deleteQuietly(logDir);
            }
        }
    }
}