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

package org.apache.eagle.hadoop.queue.storm;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataSource;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataType;

public class HadoopQueueMessageId {
    private String dataType;
    private String dataSource;
    private Long timestamp;

    public HadoopQueueMessageId(DataType dataType, DataSource dataSource, Long timestamp) {
        this.dataSource = dataSource.name();
        this.dataType = dataType.name();
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final HadoopQueueMessageId other = (HadoopQueueMessageId) obj;
        return Objects.equal(this.dataType, other.dataType)
                && Objects.equal(this.dataSource, other.dataSource)
                && Objects.equal(this.timestamp, other.timestamp);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(dataType).append(dataSource).append(timestamp).toHashCode();
    }

    @Override
    public String toString() {
        return String.format("dataType=%s, dataSource=%s, timestamp=%d", dataType, dataSource, timestamp);
    }
}
