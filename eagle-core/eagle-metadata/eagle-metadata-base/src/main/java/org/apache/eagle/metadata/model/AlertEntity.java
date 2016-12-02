/*
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

package org.apache.eagle.metadata.model;

import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("alert_detail")
@ColumnFamily("f")
@Prefix("alert_detail")
@Service(AlertConstants.ALERT_SERVICE_ENDPOINT_NAME)
@TimeSeries(true)
@Tags({"alertId", "siteId", "policyId"})
@Indexes({
        @Index(name = "Index_1_policyId", columns = { "policyId" }, unique = true)
        })
public class AlertEntity extends TaggedLogAPIEntity {
    @Column("a")
    private List<String> appIds;
    @Column("b")
    private String policyValue;
    @Column("c")
    private Map<String, Object> alertData;

    public List<String> getAppIds() {
        return appIds;
    }

    public void setAppIds(List<String> appIds) {
        this.appIds = appIds;
        valueChanged("appIds");
    }

    public String getPolicyValue() {
        return policyValue;
    }

    public void setPolicyValue(String policyValue) {
        this.policyValue = policyValue;
        valueChanged("policyValue");
    }

    public Map<String, Object> getAlertData() {
        return alertData;
    }

    public void setAlertData(Map<String, Object> alertData) {
        this.alertData = alertData;
        valueChanged("alertData");
    }

}
