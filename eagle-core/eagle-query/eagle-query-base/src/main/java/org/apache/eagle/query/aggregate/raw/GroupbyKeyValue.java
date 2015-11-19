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
package org.apache.eagle.query.aggregate.raw;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <h3>Groupby KeyValue Structure</h3>
 * <pre>
 * {
 *  key: GroupbyKey
 *  value: GroupbyValue
 * }
 * </pre>
 * @see GroupbyKey
 * @see GroupbyValue
 *
 * @since : 11/4/14,2014
 */
public class GroupbyKeyValue implements Writable {
	private GroupbyKey key;
	private GroupbyValue value;
	public GroupbyKeyValue(){
		this.key = new GroupbyKey();
		this.value = new GroupbyValue();
	}
	public GroupbyKeyValue(GroupbyKey key,GroupbyValue value){
		this.key = key;
		this.value = value;
	}
	public GroupbyKey getKey() {
		return key;
	}

	public void setKey(GroupbyKey key) {
		this.key = key;
	}

	public GroupbyValue getValue() {
		return value;
	}

	public void setValue(GroupbyValue value) {
		this.value = value;
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws java.io.IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
	}

	/**
	 * Deserialize the fields of this object from <code>in</code>.
	 * <p/>
	 * <p>For efficiency, implementations should attempt to re-use storage in the
	 * existing object where possible.</p>
	 *
	 * @param in <code>DataInput</code> to deseriablize this object from.
	 * @throws java.io.IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		this.value.readFields(in);
	}
}
