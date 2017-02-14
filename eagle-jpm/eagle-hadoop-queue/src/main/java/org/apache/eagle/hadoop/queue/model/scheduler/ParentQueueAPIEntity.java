/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.queue.model.scheduler;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("queue_map")
@ColumnFamily("f")
@Prefix("queueMap")
@Service(HadoopClusterConstants.QUEUE_MAPPING_SERVICE_NAME)
@TimeSeries(false)
@Partition( {"site"})
public class ParentQueueAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    Map<String, String> queueMap;

    public Map<String, String> getQueueMap() {
        return queueMap;
    }

    public void setQueueMap(Map<String, String> queueMap) {
        this.queueMap = queueMap;
        valueChanged("queueMap");
    }

}
