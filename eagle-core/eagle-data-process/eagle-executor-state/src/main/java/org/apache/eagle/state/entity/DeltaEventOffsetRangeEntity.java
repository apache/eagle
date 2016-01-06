/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.state.entity;

import org.apache.eagle.state.ExecutorStateConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * The starting point for the delta events since the lastest snapshot
 * Eagle uses Kafka as default storage for storing delta events, and the events can be partitioned by executorId
 * When events replay, each executor should filter the messages which belong to itself only
 */
@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
@Table("deltaEventIdRange")
@ColumnFamily("f")
@Prefix("deltaEventIdRange")
@Service(ExecutorStateConstants.POLICY_STATE_DELTA_EVENT_ID_RANGE_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site", "dataSource", "executorId"})
public class DeltaEventOffsetRangeEntity extends TaggedLogAPIEntity {
    @Column("a")
    private Long startingOffset;

    public Long getStartingOffset(){
        return startingOffset;
    }

    public void setStartingOffset(Long startingOffset){
        this.startingOffset = startingOffset;
        valueChanged("startingOffset");
    }
}
