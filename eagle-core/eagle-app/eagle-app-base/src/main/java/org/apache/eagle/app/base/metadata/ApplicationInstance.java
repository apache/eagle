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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Properties;

/**
 * Site app management entity
 */
public class ApplicationInstance {
    private String uuid;
    private Site site;
    private ApplicationSpec application;
    private Properties configurations;
    private long createdTime;
    private long modifiedTime;

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    @JsonProperty
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @JsonProperty
    public Properties getConfigurations() {
        return configurations;
    }

    public void setConfigurations(Properties configurations) {
        this.configurations = configurations;
    }

    @JsonProperty
    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public ApplicationSpec getApplication() {
        return application;
    }

    public void setApplication(ApplicationSpec application) {
        this.application = application;
    }
}