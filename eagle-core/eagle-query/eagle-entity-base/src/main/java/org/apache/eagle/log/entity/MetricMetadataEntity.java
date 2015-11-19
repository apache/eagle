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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Indexes;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Index;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.TimeSeries;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_metric")
@ColumnFamily("f")
@Prefix("dmeta")
@Service("MetricMetadataService")
@TimeSeries(false)
@Indexes({
	@Index(name="Index_1_name", columns = { "name" }, unique = true)
	})
public class MetricMetadataEntity extends TaggedLogAPIEntity {
	
	@Column("a")
	private String storeType;
	@Column("b")
	private String displayName;
	@Column("c")
	private String defaultDownSamplingFunction;
	@Column("d")
	private String defaultAggregateFunction;
	@Column("e")
	private String aggFunctions;
	@Column("f")
	private String downSamplingFunctions;
	@Column("g")
	private String resolutions;
	@Column("h")
	private String drillDownPaths;
	
	public String getStoreType() {
		return storeType;
	}
	public void setStoreType(String storeType) {
		this.storeType = storeType;
		_pcs.firePropertyChange("storeType", null, null);
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
		_pcs.firePropertyChange("displayName", null, null);
	}
	public String getDefaultDownSamplingFunction() {
		return defaultDownSamplingFunction;
	}
	public void setDefaultDownSamplingFunction(String defaultDownSamplingFunction) {
		this.defaultDownSamplingFunction = defaultDownSamplingFunction;
		_pcs.firePropertyChange("defaultDownSamplingFunction", null, null);
	}
	public String getDefaultAggregateFunction() {
		return defaultAggregateFunction;
	}
	public void setDefaultAggregateFunction(String defaultAggregateFunction) {
		this.defaultAggregateFunction = defaultAggregateFunction;
		_pcs.firePropertyChange("defaultAggregateFunction", null, null);
	}
	public String getAggFunctions() {
		return aggFunctions;
	}
	public void setAggFunctions(String aggFunctions) {
		this.aggFunctions = aggFunctions;
		_pcs.firePropertyChange("aggFunctions", null, null);
	}
	public String getDownSamplingFunctions() {
		return downSamplingFunctions;
	}
	public void setDownSamplingFunctions(String downSamplingFunctions) {
		this.downSamplingFunctions = downSamplingFunctions;
		_pcs.firePropertyChange("downSamplingFunctions", null, null);
	}
	public String getResolutions() {
		return resolutions;
	}
	public void setResolutions(String resolutions) {
		this.resolutions = resolutions;
		_pcs.firePropertyChange("resolutions", null, null);
	}
	public String getDrillDownPaths() {
		return drillDownPaths;
	}
	public void setDrillDownPaths(String drillDownPaths) {
		this.drillDownPaths = drillDownPaths;
		_pcs.firePropertyChange("drillDownPaths", null, null);
	}
	
}
