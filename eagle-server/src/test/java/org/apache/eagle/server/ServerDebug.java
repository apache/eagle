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
package org.apache.eagle.server;

import com.dumbster.smtp.SimpleSmtpServer;
import com.dumbster.smtp.SmtpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

public class ServerDebug {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerDebug.class);
    private static final int SMTP_SERVER_PORT = 5025;
    private static String serverConf = null;

    static {
        // Set application.conf
        if (ServerDebug.class.getResourceAsStream("/application-debug.conf") != null
            || ServerDebug.class.getResourceAsStream("application-debug.conf") != null) {
            LOGGER.info("config.resource = application-debug.conf");
            System.setProperty("config.resource", "application-debug.conf");
        } else {
            LOGGER.info("config.resource = application.conf");
            System.setProperty("config.resource","application.conf");
        }

        //
        String userDir = System.getProperty("user.dir");
        LOGGER.info("user.dir = {}", userDir);
        serverConf = userDir + "/eagle-server/src/test/resources/configuration.yml";
    }

    public static void main(String[] args) {
        startLocalSmtpServer(SMTP_SERVER_PORT);
        LOGGER.info("java {} server {}",ServerDebug.class.getName(),serverConf);
        ServerMain.main(new String[] {
            "server", serverConf
        });
    }

    private static void startLocalSmtpServer(int port) {
        final HashSet<String> reportedMessageIds = new HashSet<>();
        final SimpleSmtpServer smtpServer = SimpleSmtpServer.start(port);
        Thread mailReporter = new Thread() {
            final Logger logger = LoggerFactory.getLogger(SimpleSmtpServer.class);

            @Override
            public void run() {
                logger.info("Starting Local SMTP server: smtp://localhost:{}", port);
                while(!smtpServer.isStopped()) {
                    if (smtpServer.getReceivedEmailSize() > 0) {
                        smtpServer.getReceivedEmail().forEachRemaining(mail -> {
                                SmtpMessage message = (SmtpMessage) mail;
                                String msgId = message.getHeaderValue("Message-ID");
                                if (!reportedMessageIds.contains(msgId)) {
                                    logger.info("New email ID: {}, HTML: \n{}", msgId, mail.toString());
                                    reportedMessageIds.add(msgId);
                                    if(reportedMessageIds.size() > 10000) {
                                        logger.warn("Too many messages ({}) in memory, restarting",reportedMessageIds.size());
                                        smtpServer.stop();
                                        startLocalSmtpServer(port);
                                    }
                                }
                            }
                        );
                    }
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage(),e );
                    }
                }
                logger.info("Shutting down");
            }
        };
        mailReporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                smtpServer.stop();
            }
        });
    }
}
