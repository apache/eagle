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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_metadata")
@ColumnFamily("f")
@Prefix("topologyExecution")
@Service(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "topology"})
public class TopologyExecutionEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String url;
    @Column("b")
    private String deploy;
    @Column("c")
    private String status;
    @Column("d")
    private long lastUpdateTime;

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

}
