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
import org.apache.eagle.server.authentication.config.AuthenticationSettings;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ServerConfig extends Configuration {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfig.class);

    private static final String SERVER_NAME = "Apache Eagle";
    private static final String API_BASE_PATH = "/rest/*";
    private static final String CONTEXT_PATH = "/";
    private static final String RESOURCE_PACKAGE = "org.apache.eagle";
    private static final String LICENSE = "Apache License (Version 2.0)";
    private static final String LICENSE_URL = "http://www.apache.org/licenses/LICENSE-2.0";

    private AuthenticationSettings auth = new AuthenticationSettings();

    public Config getConfig() {
        return ConfigFactory.load();
    }

    @JsonProperty("auth")
    public AuthenticationSettings getAuth() {
        return auth;
    }

    @JsonProperty("auth")
    public void setAuth(AuthenticationSettings auth) {
        this.auth = auth;
    }

    static String getServerName() {
        return SERVER_NAME;
    }

    static String getProjectVersion() {
        Properties properties = loadBuildProperties();
        String projectVersion = properties.getProperty("project.version");
        return projectVersion;
    }

    static String getBuildNumber() {
        Properties properties = loadBuildProperties();
        String projectVersion = properties.getProperty("project.version");
        String timestamp = properties.getProperty("build.timestamp");
        String gitBranch = properties.getProperty("build.git.branch");
        String gitRevision = properties.getProperty("build.git.revision");

        return projectVersion + "_" + gitBranch + "_" + gitRevision + "_" + timestamp;
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

    private static Properties loadBuildProperties() {
        InputStream resourceAsStream = ServerConfig.class.getClass().getResourceAsStream( "/build.properties" );
        Properties properties = new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            LOG.warn("build.properties can not be loaded. " + e);
        }

        return properties;
    }
}
