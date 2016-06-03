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

package org.apache.eagle.service.alert;


import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.alert.entity.SiteApplicationServiceEntity;

import java.util.List;
import java.util.Map;

public class SiteApplicationObject extends TaggedLogAPIEntity {

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
        valueChanged("enabled");
    }

    public List<SiteApplicationServiceEntity> getApplications() {
        return applications;
    }

    public void setApplications(List<SiteApplicationServiceEntity> applications) {
        this.applications = applications;
        valueChanged("applicationList");
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
        valueChanged("tags");
    }

    Map<String, String> tags;
    Boolean enabled;
    List<SiteApplicationServiceEntity> applications;
}