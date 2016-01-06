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
 * policy state snapshot is stored in eagle service as an entity
 * This can be extended to writeState state for any processing element in the topology
 */
@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
@Table("policyStateSnapshot")
@ColumnFamily("f")
@Prefix("policyStateSnapshot")
@Service(ExecutorStateConstants.POLICY_STATE_SNAPSHOT_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(true)
@Tags({"site", "applicationId", "executorId"})
public class ExecutorStateSnapshotEntity extends TaggedLogAPIEntity {
    @Column("a")
    private byte[] state;

    public byte[] getState(){
        return this.state;
    }
    public void setState(byte[] state){
        this.state = state;
        valueChanged("state");
    }
}
