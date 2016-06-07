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
package org.apache.eagle.alert.engine.coordinator;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @since Apr 5, 2016
 *
 */
public class StreamingCluster {
    public static enum StreamingType {
        STORM
    }

    @JsonProperty
    private String name;
    @JsonProperty
    private String zone;
    @JsonProperty
    private StreamingType type;
    @JsonProperty
    private String description;
    /**
     * key - nimbus for storm
     */
    @JsonProperty
    private Map<String, String> deployments;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public StreamingType getType() {
        return type;
    }

    public void setType(StreamingType type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getDeployments() {
        return deployments;
    }

    public void setDeployments(Map<String, String> deployments) {
        this.deployments = deployments;
    }

    public static final String NIMBUS_HOST = "nimbusHost";
    public static final String NIMBUS_THRIFT_PORT = "nimbusThriftPort";

}
