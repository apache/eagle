/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.environment.builder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AggregateFunction implements TransformFunction {
    private String aggFieldName;
    private String resultFieldName;
    private List<String> groupByFieldNames;
    private long windowLengthMs;

    public List<String> getGroupByFieldNames() {
        return groupByFieldNames;
    }

    public void setGroupByFieldNames(List<String> groupByFieldNames) {
        this.groupByFieldNames = groupByFieldNames;
    }

    public String getResultFieldName() {
        return resultFieldName;
    }

    public void setResultFieldName(String resultFieldName) {
        this.resultFieldName = resultFieldName;
    }

    public String getAggFieldName() {
        return aggFieldName;
    }

    public void setAggFieldName(String aggFieldName) {
        this.aggFieldName = aggFieldName;
    }

    public AggregateFunction asField(String resultFieldName) {
        this.setResultFieldName(resultFieldName);
        return this;
    }

    public AggregateFunction groupBy(String... groupByFieldNames) {
        this.setGroupByFieldNames(Arrays.asList(groupByFieldNames));
        return this;
    }

    public AggregateFunction windowBy(long windowLength, TimeUnit timeUnit) {
        this.windowLengthMs = timeUnit.toMillis(windowLength);
        return this;
    }
}