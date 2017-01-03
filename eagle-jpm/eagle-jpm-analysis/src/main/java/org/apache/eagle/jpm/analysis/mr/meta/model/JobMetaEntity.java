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

package org.apache.eagle.jpm.analysis.mr.meta.model;

import org.apache.eagle.metadata.persistence.PersistenceEntity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JobMetaEntity extends PersistenceEntity {
    private String jobDefId;
    private String siteId;
    private Map<String, Object> configuration = new HashMap<>();
    private Set<String> evaluators = new HashSet<>();

    public JobMetaEntity(String jobDefId,
                         String siteId,
                         Map<String, Object> configuration,
                         Set<String> evaluators) {
        this.jobDefId = jobDefId;
        this.siteId = siteId;
        this.configuration = configuration;
        this.evaluators = evaluators;
    }

    @Override
    public String toString() {
        return String.format("JobMetaEntity[jobDefId=%s, siteId=%s]", jobDefId, siteId);
    }

    public String getJobDefId() {
        return jobDefId;
    }

    public void setJobDefId(String jobDefId) {
        this.jobDefId = jobDefId;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public Set<String> getEvaluators() {
        return evaluators;
    }

    public void setEvaluators(Set<String> evaluators) {
        this.evaluators = evaluators;
    }
}
