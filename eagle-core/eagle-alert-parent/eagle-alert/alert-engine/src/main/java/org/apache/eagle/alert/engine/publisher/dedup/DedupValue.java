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
package org.apache.eagle.alert.engine.publisher.dedup;

import com.google.common.base.Objects;

public class DedupValue {

    private long firstOccurrence;
    private String stateFieldValue;
    private long count;

    public DedupValue() {
    }

    public DedupValue(String stateFieldValue) {
        this.stateFieldValue = stateFieldValue;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getFirstOccurrence() {
        return firstOccurrence;
    }

    public void setFirstOccurrence(long firstOccurence) {
        this.firstOccurrence = firstOccurence;
    }

    public String getStateFieldValue() {
        return stateFieldValue;
    }

    public void setStateFieldValue(String stateFieldValue) {
        this.stateFieldValue = stateFieldValue;
    }

    @Override
    public boolean equals(Object dedupValue) {
        return Objects.equal(this.getStateFieldValue(), ((DedupValue) dedupValue).getStateFieldValue());
    }

    @Override
    public int hashCode() {
        return this.stateFieldValue == null ? "".hashCode() : this.stateFieldValue.hashCode();
    }

    @Override
    public String toString() {
        return String.format("DedupValue[state: %s, count: %s, first occurrence %s]",
            stateFieldValue, count, firstOccurrence);
    }

}
