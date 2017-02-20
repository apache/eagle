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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDebug {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerDebug.class);
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

        String userDir = System.getProperty("user.dir");
        LOGGER.info("user.dir = {}", userDir);
        serverConf = userDir + "/eagle-server/src/test/resources/configuration.yml";

        try {
            Class.forName(EmbeddedMailService.class.getName());
        } catch (ClassNotFoundException e) {
            // Do nothing
        }
    }

    public static void main(String[] args) {
        LOGGER.info("java {} server {}",ServerDebug.class.getName(),serverConf);
        ServerMain.main(new String[] {
            "server", serverConf
        });
    }
}
