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
package org.apache.eagle.ml.model;

import org.apache.eagle.ml.MLConstants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.log.entity.meta.TimeSeries;

/**
 * DDL for creating the table
 * create 'mlmodel', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("mlmodel")
@ColumnFamily("f")
@Prefix("mlmodel")
@Service(MLConstants.ML_MODEL_SERVICE_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site","user", "algorithm"})
public class MLModelAPIEntity extends TaggedLogAPIEntity{
	@Column("b")
	private String content;
    @Column("c")
    private long version;

    public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
		valueChanged("content");
	}

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
        valueChanged("version");
    }
}