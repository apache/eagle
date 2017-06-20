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

package org.apache.eagle.metadata.model;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.metadata.persistence.PersistenceEntity;
import org.hibernate.validator.constraints.Length;

import java.util.*;

public class PolicyEntity extends PersistenceEntity {
    @Length(min = 1, max = 50, message = "length should between 1 and 50")
    private PolicyDefinition policyProto;
    private List<String> alertPublishmentIds = new ArrayList<>();

    public PolicyDefinition getPolicyProto() {
        return policyProto;
    }

    public void setPolicyProto(PolicyDefinition policyProto) {
        this.policyProto = policyProto;
    }

    public List<String> getAlertPublishmentIds() {
        return alertPublishmentIds;
    }

    public void setAlertPublishmentIds(List<String> alertPublishmentIds) {
        this.alertPublishmentIds = alertPublishmentIds;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(policyProto)
                .append(alertPublishmentIds)
                .build();
    }

    @Override
    public boolean equals(Object that) {
        if (that == this) {
            return true;
        }

        if (!(that instanceof PolicyEntity)) {
            return false;
        }

        PolicyEntity another = (PolicyEntity) that;

        return Objects.equals(another.policyProto, this.policyProto)
                && CollectionUtils.isEqualCollection(another.getAlertPublishmentIds(), alertPublishmentIds);
    }

    @Override
    public String toString() {
        return String.format("{name=\"%s\",definition=%s}", this.getPolicyProto().getName(), this.getPolicyProto() == null ? "null" : this.getPolicyProto().getDefinition().toString());
    }

}
