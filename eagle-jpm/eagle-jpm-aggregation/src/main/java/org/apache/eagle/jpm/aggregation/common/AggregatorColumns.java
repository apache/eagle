/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.aggregation.common;

import java.util.List;

public class AggregatorColumns implements Comparable<AggregatorColumns> {
    private List<String> columnNames;
    private List<String> columnValues;

    public AggregatorColumns(List<String> columnNames, List<String> columnValues) {
        this.columnNames = columnNames;
        this.columnValues = columnValues;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnValues() {
        return columnValues;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String columnValue : columnValues) {
            sb.append(columnValue);
            sb.append(",");
        }

        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    @Override
    public int compareTo(AggregatorColumns o) {
        if (this.columnValues.size() > o.columnValues.size()) {
            return 1;
        } else if (this.columnValues.size() < o.columnValues.size()) {
            return -1;
        } else {
            return this.toString().compareTo(o.toString());
        }
    }
}
