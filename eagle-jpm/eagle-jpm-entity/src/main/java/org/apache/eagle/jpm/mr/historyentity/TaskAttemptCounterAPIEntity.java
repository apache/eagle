/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.historyentity;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa_anomaly")
@ColumnFamily("f")
@Prefix("tacount")
@Service(Constants.JPA_TASK_ATTEMPT_COUNTER_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
public class TaskAttemptCounterAPIEntity extends JobBaseAPIEntity {
    @Column("a")
    private int totalCount;
    @Column("b")
    private int failedCount;
    @Column("c")
    private int killedCount;
    
    public int getKilledCount() {
        return killedCount;
    }

    public void setKilledCount(int killedCount) {
        this.killedCount = killedCount;
        pcs.firePropertyChange("killedCount", null, null);
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
        pcs.firePropertyChange("failedCount", null, null);
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
        pcs.firePropertyChange("totalCount", null, null);
    }
}
