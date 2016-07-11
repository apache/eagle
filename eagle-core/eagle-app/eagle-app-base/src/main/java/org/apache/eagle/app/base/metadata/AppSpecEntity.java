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
package org.apache.eagle.app.base.metadata;

import org.apache.eagle.app.base.App;

import java.util.List;

/**
 * Static metadata provided by installed apps
 */
public class AppSpecEntity {
    private String name;
    private String version;
    private String description;
    private Class<App> appClass;
    private List<ConfigurationSpec> configurations;
    private String type;
    private String jar;
    private String view;

    public Class<App> getAppClass() {
        return appClass;
    }

    public void setAppClass(Class<App> appClass) {
        this.appClass = appClass;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ConfigurationSpec> getConfigurations() {
        return configurations;
    }

    public void setConfigurations(List<ConfigurationSpec> configurations) {
        this.configurations = configurations;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getJar() {
        return jar;
    }

    public void setJar(String jar) {
        this.jar = jar;
    }

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }
}