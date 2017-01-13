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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa_anomaly")
@ColumnFamily("f")
@Prefix("taskfailurecount")
@Service(Constants.JPA_TASK_FAILURE_COUNT_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
public class TaskFailureCountAPIEntity extends JobBaseAPIEntity {
    @Column("a")
    private int failureCount;
    @Column("b")
    private String error;
    @Column("c")
    private String taskStatus;

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
        pcs.firePropertyChange("taskStatus", null, null);
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
        pcs.firePropertyChange("error", null, null);
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
        pcs.firePropertyChange("failureCount", null, null);
    }
}
