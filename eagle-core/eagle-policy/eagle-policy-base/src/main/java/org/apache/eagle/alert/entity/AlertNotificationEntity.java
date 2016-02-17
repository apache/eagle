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

package org.apache.eagle.alert.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertNotifications")
@ColumnFamily("f")
@Prefix("alertNotifications")
@Service(Constants.ALERT_NOTIFICATION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"notificationType"})
public class AlertNotificationEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String className;
    public String getClassName(){
        return className;
    }
    public void setClassName(String className){
        this.className = className;
        valueChanged("className");
    }

    @Column("b")
    private String description;
    public String getDescription(){
        return description;
    }
    public void setDescription(String description){
        this.description = description;
        valueChanged("description");
    }

    @Column("c")
    private boolean enabled;
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        valueChanged("enabled");
    }
}
