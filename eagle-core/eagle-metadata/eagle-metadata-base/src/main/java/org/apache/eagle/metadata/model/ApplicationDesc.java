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
package org.apache.eagle.metadata.model;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

import java.io.Serializable;
import java.util.List;

/**
 * Static metadata provided by installed apps.
 */
public class ApplicationDesc implements Serializable {
    private String type;
    private String name;
    private String version;
    private String description;
    private Class<?> appClass;
    private String jarPath;
    private String viewPath;
    private Class<?> providerClass;
    private Configuration configuration;
    private List<StreamDefinition> streams;
    private ApplicationDocs docs;
    private boolean executable;

    public boolean isExecutable() {
        return executable;
    }

    public void setExecutable(boolean executable) {
        this.executable = executable;
    }

    private List<ApplicationDependency> dependencies;

    public String getDescription() {
        return description;
    }

    public String getVersion() {
        return version;
    }

    public String getType() {
        return type;
    }

    public String getJarPath() {
        return jarPath;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getName() {
        return name;
    }

    public String getViewPath() {
        return viewPath;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setViewPath(String viewPath) {
        this.viewPath = viewPath;
    }

    public Class<?> getAppClass() {
        return appClass;
    }

    public void setAppClass(Class<?> appClass) {
        this.appClass = appClass;
    }

    public Class<?> getProviderClass() {
        return providerClass;
    }

    public void setProviderClass(Class<?> providerClass) {
        this.providerClass = providerClass;
    }

    @Override
    public String toString() {
        return String.format("ApplicationDesc [type=%s, name=%s, version=%s, appClass=%s, viewPath=%s, jarpath=%s, providerClass=%s, configuration= %s properties, description=%s",
            getType(), getName(), getVersion(), getAppClass(), getViewPath(),getJarPath(), getProviderClass(), getConfiguration() == null ? 0 : getConfiguration().size(), getDescription());
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public List<StreamDefinition> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamDefinition> streams) {
        this.streams = streams;
    }

    public ApplicationDocs getDocs() {
        return docs;
    }

    public void setDocs(ApplicationDocs docs) {
        this.docs = docs;
    }

    public List<ApplicationDependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<ApplicationDependency> dependencies) {
        this.dependencies = dependencies;
    }
}
