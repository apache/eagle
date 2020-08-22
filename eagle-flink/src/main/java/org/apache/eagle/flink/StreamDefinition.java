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
package org.apache.eagle.flink;

import lombok.Builder;
import lombok.Data;
import java.io.Serializable;
import java.util.List;

@Builder(toBuilder = true)
@Data
public class StreamDefinition implements Serializable {
    // Stream unique ID
    private String streamId;

    // Stream description
    private String description;

    // Is validateable or not
    private boolean validate;

    // Is timeseries-based stream or not
    private boolean timeseries;

    // Stream data source ID
    private String dataSource;

    private String group;

    private String streamSource;

    // Tenant (Site) ID
    private String siteId;

    private List<StreamColumn> columns;

    public int getColumnIndex(String column) {
        int i = 0;
        for (StreamColumn col : this.getColumns()) {
            if (col.getName().equals(column)) {
                return i;
            }
            i++;
        }
        return -1;
    }

}