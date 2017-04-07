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

package org.apache.eagle.alert.engine.coordinator;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Objects;

public class AlertDeduplication {
    private String dedupIntervalMin;
    private List<String> dedupFields;

    public String getDedupIntervalMin() {
        return dedupIntervalMin;
    }

    public void setDedupIntervalMin(String dedupIntervalMin) {
        this.dedupIntervalMin = dedupIntervalMin;
    }

    public List<String> getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(List<String> dedupFields) {
        this.dedupFields = dedupFields;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(dedupFields)
                .append(dedupIntervalMin)
                .build();
    }

    @Override
    public boolean equals(Object that) {
        if (that == this) {
            return true;
        }
        if (!(that instanceof AlertDeduplication)) {
            return false;
        }
        AlertDeduplication another = (AlertDeduplication) that;
        if (ListUtils.isEqualList(another.dedupFields, this.dedupFields)
                && Objects.equals(another.dedupIntervalMin, this.dedupIntervalMin)) {
            return true;
        }
        return false;
    }


}
