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

package org.apache.eagle.alert.engine.publisher.dedup;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Objects;

public class DedupKey {
    private String policyId;
    private String outputStreamId;

    public DedupKey(String policyId, String outputStreamId) {
        this.policyId = policyId;
        this.outputStreamId = outputStreamId;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getOutputStreamId() {
        return outputStreamId;
    }

    public void setOutputStreamId(String outputStreamId) {
        this.outputStreamId = outputStreamId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof DedupKey) {
            DedupKey au = (DedupKey) obj;
            return Objects.equals(au.getOutputStreamId(), this.outputStreamId)
                    && Objects.equals(au.getPolicyId(), this.policyId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(outputStreamId).append(policyId).build();
    }

    @Override
    public String toString() {
        return String.format("DedupKey[outputStreamId: %s, policyId: %s]", outputStreamId, policyId);
    }
}
