/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.userprofile.model;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

@Service(ScheduleCommandEntity.ScheduleTaskService)
@Table("userprofile")
@Prefix("schedule_command")
@ColumnFamily("f")
@TimeSeries(true)
@Tags({"site","type"})
public class ScheduleCommandEntity extends TaggedLogAPIEntity {
    public static final String ScheduleTaskService = "ScheduleTaskService";
    public static enum STATUS {
        INITIALIZED, PENDING,EXECUTING, SUCCEEDED, FAILED
        // , CANCELED
    }

    @Column("a")
    private String status;
    public static String getScheduleTaskService() {
        return ScheduleTaskService;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
        valueChanged("updateTime");
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
        valueChanged("detail");
    }

    @Column("b")
    private String detail;
    @Column("c")
    private long updateTime;

}