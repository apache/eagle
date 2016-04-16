/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.application.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;

import org.apache.eagle.service.application.AppManagerConstants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.Map;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_metadata")
@ColumnFamily("f")
@Prefix("topologyExecution")
@Service(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "application", "topology"})
public class TopologyExecutionEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String fullName;
    @Column("b")
    private String url;
    @Column("c")
    private String description;
    @Column("d")
    private String status;
    @Column("e")
    private long lastModifiedDate;
    @Column("f")
    private String mode;
    @Column("g")
    private String environment;

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
        valueChanged("environment");
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
        valueChanged("mode");
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
        valueChanged("fullName");
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
        valueChanged("url");
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
        valueChanged("description");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public long getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(long lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
        valueChanged("lastModifiedDate");
    }

    public String getSite() {
        return this.getTags().get(AppManagerConstants.SITE_TAG);
    }

    public String getApplication() {
        return this.getTags().get(AppManagerConstants.APPLICATION_TAG);
    }

    public String getTopology() {
        return this.getTags().get(AppManagerConstants.TOPOLOGY_TAG);
    }

}
