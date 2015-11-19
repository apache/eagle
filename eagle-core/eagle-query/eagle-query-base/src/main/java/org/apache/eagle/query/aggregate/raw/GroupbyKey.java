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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

/**
 * <h3>Structure</h3>
 * <pre>
 * {
 *   List[byte[],...]
 * }
 * </pre>
 */
public class GroupbyKey implements Writable {
	private final WritableList<BytesWritable> value;

	public void addValue(byte[] value){
		this.value.add(new BytesWritable(value));
	}
	public void addAll(List<BytesWritable> list){
		this.value.addAll(list);
	}

	public List<BytesWritable> getValue(){
		return value;
	}

	/**
	 * empty constructor
	 */
	public GroupbyKey(){
		this.value = new WritableList<BytesWritable>(BytesWritable.class);
	}

	/**
	 * clear for reuse
	 */
	public void clear(){
		value.clear();
	}

	/**
	 * copy constructor
	 * @param key
	 */
	public GroupbyKey(GroupbyKey key){
		this();
		ListIterator<BytesWritable> it = key.value.listIterator();
//		ListIterator<byte[]> it = key.value.listIterator();
		while(it.hasNext()){
			this.value.add(it.next());
		}
	}

	public GroupbyKey(List<byte[]> bytes){
		this();
		for(byte[] bt:bytes){
			this.addValue(bt);
		}
	}

	@Override
	public boolean equals(Object obj){
		if(obj == this)
			return true;
		if(!(obj instanceof GroupbyKey)){
			return false;
		}
		GroupbyKey that = (GroupbyKey)obj;
//		ListIterator<byte[]> e1 = this.value.listIterator();
//		ListIterator<byte[]> e2 = that.value.listIterator();
		ListIterator<BytesWritable> e1 = this.value.listIterator();
		ListIterator<BytesWritable> e2 = that.value.listIterator();
		while(e1.hasNext() && e2.hasNext()){
			if(!Arrays.equals(e1.next().getBytes(), e2.next().getBytes()))
				return false;
		}
		return !(e1.hasNext() || e2.hasNext());
	}

	@Override
	public int hashCode(){
		ListIterator<BytesWritable> e1 = this.value.listIterator();
		int hash = 0xFFFFFFFF;
		while(e1.hasNext()){
			hash ^= Arrays.hashCode(e1.next().getBytes());
		}
		return hash;
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws java.io.IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
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
		this.value.readFields(in);
	}
}
