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

package org.apache.eagle.app.utils;

import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;

/**
 * Application Execution Must-have base configuration.
 */
public class ApplicationExecutionConfig {
    public static final String APP_ID_KEY = "appId";
    public static final String MODE_KEY = "mode";
    public static final String SITE_ID_KEY = "siteId";
    public static final String JAR_PATH_KEY = "jarPath";

    private final String siteId;
    private final String mode;
    private final String appId;
    private final String jarPath;

    public ApplicationExecutionConfig(ApplicationEntity metadata) {
        this.siteId = metadata.getSite().getSiteId();
        this.mode = metadata.getMode().name();
        this.appId = metadata.getAppId();
        this.jarPath = metadata.getJarPath();
    }

    public ApplicationExecutionConfig(Config config) {
        this.siteId = config.getString(SITE_ID_KEY);
        this.mode = config.getString(MODE_KEY);
        this.appId = config.getString(APP_ID_KEY);
        this.jarPath = config.getString(JAR_PATH_KEY);
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getAppId() {
        return appId;
    }

    public String getMode() {
        return mode;
    }

    public String getSiteId() {
        return siteId;
    }
}
