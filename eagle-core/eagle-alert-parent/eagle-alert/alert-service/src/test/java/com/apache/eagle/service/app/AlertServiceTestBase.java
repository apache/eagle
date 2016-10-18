/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.apache.eagle.service.app;

import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.apache.eagle.service.app.ServiceApp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Start embedded zookeeper, coordinator and alert service.
 */
public class AlertServiceTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AlertServiceTestBase.class);

    private static ZookeeperEmbedded zkEmbed;
    private static int zkPort;
    private static int serverPort;
    private static Thread serverThread;

    @BeforeClass
    public static void startZookeeperAndServiceServer() throws Throwable {
        zkEmbed = new ZookeeperEmbedded(2181);
        zkPort = zkEmbed.start();
        System.setProperty("coordinator.zkConfig.zkQuorum", "localhost:" + zkPort);

        System.setProperty("dw.server.applicationConnectors[0].port", "7777");
        System.setProperty("dw.server.adminConnectors[0].port", "7771");
        serverPort = tryStartServerApp(8080);

        LOG.info("Started Zookeeper on port {} and Service on port {}", zkPort, serverPort);
    }

    /**
     * @param initialPort initial server port
     * @return bind server port
     * @throws Exception throws if failed more then 3 times.
     */
    private static int tryStartServerApp(int initialPort) throws Throwable {
        final AtomicInteger port = new AtomicInteger(initialPort);
        final AtomicBoolean success = new AtomicBoolean(false);
        serverThread = new Thread(() -> {
            String userDir = System.getProperty("user.dir");
            String serverConf = userDir + "/src/test/resources/configuration.yml";
            Throwable lastException = null;
            int tried = 0;
            while (!success.get() && tried < 3) {
                LOG.info("Starting server in port {}", port);
                System.setProperty("dw.server.applicationConnectors[0].port", String.valueOf(port.intValue()));
                System.setProperty("dw.server.adminConnectors[0].port", String.valueOf(port.intValue() + 1));
                try {
                    new ServiceApp().run(new String[] {"server", serverConf});
                    success.set(true);
                } catch (Throwable e) {
                    LOG.warn(e.getMessage(), e);
                    tried++;
                    port.incrementAndGet();
                    lastException = e;
                }
            }
            if (!success.get()) {
                LOG.error("Failed to start server after tried 3 times");
                throw new IllegalStateException(lastException);
            }
        });
        serverThread.start();
        serverThread.join();
        return port.get();
    }

    public static int getBindServerPort() {
        return serverPort;
    }

    public static int getBindZkPort() {
        return zkPort;
    }

    @AfterClass
    public static void shutdownZookeeperAndServiceServer() throws Exception {
        if (zkEmbed != null) {
            zkEmbed.shutdown();
        }
        if (serverThread != null && serverThread.isAlive()) {
            serverThread.interrupt();
        }
    }
}
