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
package org.apache.eagle.log.entity.test;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("unittest")
@ColumnFamily("f")
@Prefix("entityut")
@Service("TestLogAPIEntity")
@TimeSeries(false)
@Indexes({
	@Index(name="jobIdIndex1", columns = { "jobID" }, unique = true),
	@Index(name="jobIdNonIndex2", columns = { "hostname" }, unique = false)
	})
public class TestLogAPIEntity extends TaggedLogAPIEntity {

	@Column("a")
	private int field1;
	@Column("b")
	private Integer field2;
	@Column("c")
	private long field3;
	@Column("d")
	private Long field4;
	@Column("e")
	private double field5;
	@Column("f")
	private Double field6;
	@Column("g")
	private String field7;
	
	public int getField1() {
		return field1;
	}
	public void setField1(int field1) {
		this.field1 = field1;
		_pcs.firePropertyChange("field1", null, null);
	}
	public Integer getField2() {
		return field2;
	}
	public void setField2(Integer field2) {
		this.field2 = field2;
		_pcs.firePropertyChange("field2", null, null);
	}
	public long getField3() {
		return field3;
	}
	public void setField3(long field3) {
		this.field3 = field3;
		_pcs.firePropertyChange("field3", null, null);
	}
	public Long getField4() {
		return field4;
	}
	public void setField4(Long field4) {
		this.field4 = field4;
		_pcs.firePropertyChange("field4", null, null);
	}
	public double getField5() {
		return field5;
	}
	public void setField5(double field5) {
		this.field5 = field5;
		_pcs.firePropertyChange("field5", null, null);
	}
	public Double getField6() {
		return field6;
	}
	public void setField6(Double field6) {
		this.field6 = field6;
		_pcs.firePropertyChange("field6", null, null);
	}
	public String getField7() {
		return field7;
	}
	public void setField7(String field7) {
		this.field7 = field7;
		_pcs.firePropertyChange("field7", null, null);
	}
}
