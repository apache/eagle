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

package org.apache.eagle.security.hbase;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("hbaseResourceSensitivity")
@ColumnFamily("f")
@Prefix("hbaseResourceSensitivity")
@Service("HbaseResourceSensitivityService")
@TimeSeries(false)
@Tags({"site", "hbaseResource"})
public class HbaseResourceSensitivityAPIEntity extends TaggedLogAPIEntity {
	private static final long serialVersionUID = 2L;
	/**
     * sensitivityType can be multi-value attribute, and values can be separated by "|"
     */
    @Column("a")
    private String sensitivityType;

    public String getSensitivityType() {
        return sensitivityType;
    }

    public void setSensitivityType(String sensitivityType) {
        this.sensitivityType = sensitivityType;
        valueChanged("sensitivityType");
    }
}
