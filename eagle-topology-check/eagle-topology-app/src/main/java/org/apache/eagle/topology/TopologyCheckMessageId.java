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

package org.apache.eagle.topology;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class TopologyCheckMessageId {
    private String topologyType;
    private Long timestamp;

    public TopologyCheckMessageId(TopologyConstants.TopologyType topologyType, Long timestamp) {
        this.topologyType = topologyType.toString();
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TopologyCheckMessageId other = (TopologyCheckMessageId) obj;
        return Objects.equal(this.topologyType, other.topologyType)
                && Objects.equal(this.timestamp, other.timestamp);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(topologyType).append(timestamp).toHashCode();
    }

    @Override
    public String toString() {
        return String.format("topologyType=%s, timestamp=%d", topologyType, timestamp);
    }
}
