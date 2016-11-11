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

package org.apache.eagle.service.client;

import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;

import java.io.Serializable;

/**
 * Some common codes to enable DAO through eagle service including service host/post, credential population etc.
 */
public class EagleServiceConnector implements Serializable {
    private final String eagleServiceHost;
    private final Integer eagleServicePort;
    private String username;
    private String password;

    public String getEagleServiceHost() {
        return this.eagleServiceHost;
    }

    public Integer getEagleServicePort() {
        return this.eagleServicePort;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public EagleServiceConnector(String eagleServiceHost, Integer eagleServicePort) {
        this(eagleServiceHost, eagleServicePort, null, null);
    }

    public EagleServiceConnector(String eagleServiceHost, Integer eagleServicePort, String username, String password) {
        this.eagleServiceHost = eagleServiceHost;
        this.eagleServicePort = eagleServicePort;
        this.username = username;
        this.password = password;
    }

    public EagleServiceConnector(Config config) {
        this.eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        this.eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        if (config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) && config.hasPath(EagleConfigConstants.EAGLE_PROPS + ""
            + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD)) {
            this.username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
            this.password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);
        }
    }
}
