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

import org.apache.eagle.common.ByteUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <h3>Strucutre</h3>
 * <pre>
 * {
 *   value: List[byte[],...],
 *   meta : List[byte[],...] // byte[] may be serialized value
 *                           // of any type of value like:
 *                           // Integer,Double or Object
 *                           // and so on
 * }
 * </pre>
 *
 * TODO: Add self-described serializer or deserializer for meta bytes array, so that any side of the RPC will know how to read/write meta information
 *
 * @since : 11/4/14,2014
 */
public class GroupbyValue implements Writable{
	private final WritableList<DoubleWritable> value;
	private WritableList<BytesWritable> meta;
	private int initialCapacity=1;
	public GroupbyValue(){
		this(1);
	}
	/**
	 * Constructs an empty list with the specified initial capacity.
	 *
	 * @param   initialCapacity   the initial capacity of the list
	 * @exception IllegalArgumentException if the specified initial capacity
	 *            is negative
	 */
	public GroupbyValue(int initialCapacity ){
		this.initialCapacity = initialCapacity;
		this.value = new WritableList<DoubleWritable>(DoubleWritable.class,this.initialCapacity);
		this.meta = new WritableList<BytesWritable>(BytesWritable.class,this.initialCapacity);
	}

	public WritableList<DoubleWritable> getValue(){
		return this.value;
	}

	public WritableList<BytesWritable> getMeta(){
		return this.meta;
	}

	public DoubleWritable get(int index){
		return this.value.get(index);
	}

	public BytesWritable getMeta(int index){
		if(this.meta==null) return null;
		return this.meta.get(index);
	}

	// Values
	public void add(DoubleWritable value){
		this.value.add(value);
	}
	public void add(Double value){
		this.value.add(new DoubleWritable(value));
	}

	public void set(int index,DoubleWritable value){
		this.value.set(index, value);
	}

	//////////////
	// Meta
	/////////////
	public void addMeta(BytesWritable meta){
		this.meta.add(meta);
	}

	public void addMeta(int meta){
		this.meta.add(new BytesWritable(ByteUtil.intToBytes(meta)));
	}

	public void setMeta(int index,BytesWritable meta){
		this.meta.set(index,meta);
	}
	public void setMeta(int index,int meta){
		this.meta.set(index, new BytesWritable(ByteUtil.intToBytes(meta)));
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws java.io.IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.initialCapacity);
		this.value.write(out);
		this.meta.write(out);
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
		this.initialCapacity = in.readInt();
		this.value.readFields(in);
		this.meta.readFields(in);
	}
}