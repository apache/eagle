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
package org.apache.eagle.alert.engine.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.utils.DateTimeUtil;

/**
 * streamId stands for alert type instead of source event streamId
 */
public class AlertStreamEvent extends StreamEvent {
    private static final long serialVersionUID = 2392131134670106397L;

    // TODO: Keep policy name only instead of policy entity
    private PolicyDefinition policy;
    private StreamDefinition schema;
    private String createdBy;
    private long createdTime;

    public PolicyDefinition getPolicy() {
        return policy;
    }

    public void setPolicy(PolicyDefinition policy) {
        this.policy = policy;
    }


    public String getPolicyId() {
        return policy.getName();
    }

    @Override
    public String toString() {
        List<String> dataStrings = new ArrayList<>(this.getData().length);
        for(Object obj: this.getData()){
            if(obj!=null) {
                dataStrings.add(obj.toString());
            }else{
                dataStrings.add(null);
            }
        }
        return String.format("AlertStreamEvent[stream=%S,timestamp=%s,data=[%s], policy=%s, createdBy=%s]",
                this.getStreamId(), DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.getTimestamp()), StringUtils.join(dataStrings,","),this.getPolicy().getName(),this.getCreatedBy());
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public StreamDefinition getSchema() {
        return schema;
    }

    public void setSchema(StreamDefinition schema) {
        this.schema = schema;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }
}
