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
@Prefix("topologyOperation")
@Service(Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site", "application", "topology", "operationID", "operation"})
public class TopologyOperationEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String status;
    @Column("b")
    private String message;
    @Column("c")
    private long lastModifiedDate;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
        valueChanged("message");
    }

    public long getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(long lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
        valueChanged("lastModifiedDate");
    }

    public final static class OPERATION {
        public final static String START = "START";
        public final static String STOP = "STOP";
        public final static String STATUS = "STATUS";
    }

    public final static class OPERATION_STATUS {
        public final static String PENDING = "PENDING";
        public final static String INITIALIZED = "INITIALIZED";
        public final static String SUCCESS = "SUCCESS";
        public final static String FAILED = "FAILED";
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

    public String getOperation() {
        return this.getTags().get(AppManagerConstants.OPERATION_TAG);
    }

}
