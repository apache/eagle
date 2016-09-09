/**
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
package org.apache.eagle.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dropwizard.Configuration;

public class ServerConfig extends Configuration {
    private static final String SERVER_NAME = "Apache Eagle";
    private static final String SERVER_VERSION = "0.5.0-incubating";
    private static final String API_BASE_PATH = "/rest/*";
    private static final String CONTEXT_PATH = "/";
    private static final String RESOURCE_PACKAGE = "org.apache.eagle";
    private static final String LICENSE = "Apache License (Version 2.0)";
    private static final String LICENSE_URL = "http://www.apache.org/licenses/LICENSE-2.0";

    public Config getConfig() {
        return ConfigFactory.load();
    }

    static String getServerName() {
        return SERVER_NAME;
    }

    static String getServerVersion() {
        return SERVER_VERSION;
    }

    static String getApiBasePath() {
        return API_BASE_PATH;
    }

    static String getResourcePackage() {
        return RESOURCE_PACKAGE;
    }

    static String getContextPath() {
        return CONTEXT_PATH;
    }

    public static String getLicense() {
        return LICENSE;
    }

    static String getLicenseUrl() {
        return LICENSE_URL;
    }
}