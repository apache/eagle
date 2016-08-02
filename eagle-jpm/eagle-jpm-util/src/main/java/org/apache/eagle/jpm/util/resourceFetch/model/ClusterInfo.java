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
package org.apache.eagle.jpm.util.resourceFetch.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private long id;
    private long startedOn;
    private String state;
    private String haState;
    private String resourceManagerVersion;
    private String resourceManagerBuildVersion;
    private String resourceManagerVersionBuiltOn;
    private String hadoopVersion;
    private String hadoopBuildVersion;
    private String hadoopVersionBuiltOn;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getStartedOn() {
        return startedOn;
    }

    public void setStartedOn(long startedOn) {
        this.startedOn = startedOn;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getHaState() {
        return haState;
    }

    public void setHaState(String haState) {
        this.haState = haState;
    }

    public String getResourceManagerVersion() {
        return resourceManagerVersion;
    }

    public void setResourceManagerVersion(String resourceManagerVersion) {
        this.resourceManagerVersion = resourceManagerVersion;
    }

    public String getResourceManagerBuildVersion() {
        return resourceManagerBuildVersion;
    }

    public void setResourceManagerBuildVersion(String resourceManagerBuildVersion) {
        this.resourceManagerBuildVersion = resourceManagerBuildVersion;
    }

    public String getResourceManagerVersionBuiltOn() {
        return resourceManagerVersionBuiltOn;
    }

    public void setResourceManagerVersionBuiltOn(String resourceManagerVersionBuiltOn) {
        this.resourceManagerVersionBuiltOn = resourceManagerVersionBuiltOn;
    }

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    public void setHadoopVersion(String hadoopVersion) {
        this.hadoopVersion = hadoopVersion;
    }

    public String getHadoopBuildVersion() {
        return hadoopBuildVersion;
    }

    public void setHadoopBuildVersion(String hadoopBuildVersion) {
        this.hadoopBuildVersion = hadoopBuildVersion;
    }

    public String getHadoopVersionBuiltOn() {
        return hadoopVersionBuiltOn;
    }

    public void setHadoopVersionBuiltOn(String hadoopVersionBuiltOn) {
        this.hadoopVersionBuiltOn = hadoopVersionBuiltOn;
    }
}
