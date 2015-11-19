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
package org.apache.eagle.query.aggregate.timeseries;

/**
 * sum(field1), max(field2) groupby(field3, field4) sort by field1 asc, field3 desc
 * There are 2 SortOption object, then
 * the 1st one is inGroupby=false, index=0, ascendent=true
 * the 2nd one is inGroupby=true, index=1, ascendent=false
 *
 */
public class SortOption {
	private boolean inGroupby; // sort field defaultly is not from groupby fields 
	private int index; // index relative to list of groupby fields or list of functions
	private boolean ascendant; //asc or desc

	public boolean isInGroupby() {
		return inGroupby;
	}
	public void setInGroupby(boolean inGroupby) {
		this.inGroupby = inGroupby;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public boolean isAscendant() {
		return ascendant;
	}
	public void setAscendant(boolean ascendant) {
		this.ascendant = ascendant;
	}
}
