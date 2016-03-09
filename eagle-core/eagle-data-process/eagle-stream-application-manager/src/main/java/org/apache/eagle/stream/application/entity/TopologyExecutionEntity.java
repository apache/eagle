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

package org.apache.eagle.stream.application.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.stream.application.AppManagerConstants;
import org.apache.eagle.stream.application.model.TopologyExecutionModel;
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
    private String deploy;
    @Column("d")
    private String status;
    @Column("e")
    private long lastUpdateTime;

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
        valueChanged("url");
    }

    public String getDeploy() {
        return deploy;
    }

    public void setDeploy(String deploy) {
        this.deploy = deploy;
        valueChanged("deploy");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        valueChanged("lastUpdateTime");
    }

    public final static class TOPOLOGY_STATUS {
        public final static String STOPPED = "STOPPED";
        public final static String STARTED = "STARTED";
        public final static String PENDING = "PENDING";
        public final static String NEW = "NEW";
    }

    public static TopologyExecutionModel toModel(final TopologyExecutionEntity entity){
        TopologyExecutionModel model = new TopologyExecutionModel (
                entity.getTags().get(AppManagerConstants.SITE_TAG),
                entity.getTags().get(AppManagerConstants.APPLICATION_TAG),
                entity.getTags().get(AppManagerConstants.TOPOLOGY_TAG),
                entity.getFullName(),
                entity.getUrl(),
                entity.getDeploy(),
                entity.getStatus(),
                entity.getLastUpdateTime());
        return model;
    }

    public static TopologyExecutionEntity fromModel(final TopologyExecutionModel model){
        TopologyExecutionEntity entity = new TopologyExecutionEntity();
        Map<String,String> tags = new HashMap<String,String>(){{
            put(AppManagerConstants.SITE_TAG, model.site());
            put(AppManagerConstants.APPLICATION_TAG, model.application());
            put(AppManagerConstants.TOPOLOGY_TAG, model.topology());
        }};
        entity.setFullName(model.fullName());
        entity.setUrl(model.url());
        entity.setDeploy(model.deploy());
        entity.setStatus(model.status());
        entity.setLastUpdateTime(model.lastUpdateTime());
        entity.setTags(tags);
        return entity;
    }
}
