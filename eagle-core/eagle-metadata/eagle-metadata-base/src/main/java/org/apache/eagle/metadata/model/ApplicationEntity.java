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

import org.apache.eagle.metadata.persistence.PersistenceEntity;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Site app management entity.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationEntity extends PersistenceEntity {
    private String appId;
    private SiteEntity site;
    private ApplicationDesc descriptor;
    private boolean executable = true;

    private Map<String, Object> configuration = new HashMap<>();
    private Map<String, String> context = new HashMap<>();
    private List<StreamDesc> streams;
    private Mode mode = Mode.CLUSTER;
    private String jarPath;
    private Status status = Status.INITIALIZED;

    public ApplicationEntity() {
    }

    public ApplicationEntity(String siteId, String appType) {
        this.site = new SiteEntity("", siteId);
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType(appType);
        this.descriptor = applicationDesc;
        this.mode = null;
        this.status = null;
    }

    public ApplicationEntity(SiteEntity site, ApplicationDesc descriptor, Mode mode, Status status, String uuid, String appId) {
        this.site = site;
        this.descriptor = descriptor;
        this.mode = mode;
        this.status = status;
        this.setUuid(uuid);
        this.appId = appId;
    }


    public SiteEntity getSite() {
        return site;
    }

    public void setSite(SiteEntity site) {
        this.site = site;
    }

    public ApplicationDesc getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(ApplicationDesc descriptor) {
        this.descriptor = descriptor;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public void ensureDefault() {
        super.ensureDefault();
        if (this.appId == null) {
            this.appId = String.format("%s-%s", this.getDescriptor().getType(), this.getSite().getSiteId());
        }
        if (this.status == null) {
            this.status = Status.INITIALIZED;
        }
    }

    public Map<String, String> getContext() {
        return context;
    }

    public void setContext(Map<String, String> context) {
        this.context = context;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<StreamDesc> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamDesc> streams) {
        this.streams = streams;
    }

    public boolean isExecutable() {
        return executable;
    }

    public void setExecutable(boolean executable) {
        this.executable = executable;
    }

    public static enum Status {
        INITIALIZED("INITIALIZED"),
        STARTING("STARTING"),
        RUNNING("RUNNING"),
        STOPPING("STOPPING"),
        STOPPED("STOPPED");

        private final String status;

        Status(String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return status;
        }
    }

    public static enum Mode {
        LOCAL("LOCAL"),
        CLUSTER("CLUSTER");
        private final String name;

        Mode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
