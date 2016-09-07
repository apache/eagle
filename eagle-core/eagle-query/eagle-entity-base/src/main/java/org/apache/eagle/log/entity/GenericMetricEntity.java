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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * GenericMetricEntity should use prefix field which is extended from TaggedLogAPIEntity as metric name
 * metric name is used to partition the metric tables
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_metric")
@ColumnFamily("f")
@Prefix(GenericMetricEntity.GENERIC_METRIC_PREFIX_PLACE_HOLDER)
@Service(GenericMetricEntity.GENERIC_METRIC_SERVICE)
@TimeSeries(true)
@Metric(interval=60000)
@ServicePath(path = "/metric")
// TODO:
@Tags({"site","application","policyId","alertExecutorId", "streamName","source","partitionSeq"})
public class GenericMetricEntity extends TaggedLogAPIEntity {
	public static final String GENERIC_METRIC_SERVICE = "GenericMetricService";
	public static final String GENERIC_METRIC_PREFIX_PLACE_HOLDER = "GENERIC_METRIC_PREFIX_PLACEHODLER";
	public static final String VALUE_FIELD ="value";

	@Column("a")
	private double[] value;

	public double[] getValue() {
		return value;
	}

	public void setValue(double[] value) {
		this.value = value;
		pcs.firePropertyChange("value", null, null);
	}
}