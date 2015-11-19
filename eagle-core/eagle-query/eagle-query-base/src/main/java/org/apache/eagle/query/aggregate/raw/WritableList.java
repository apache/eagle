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
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;

/**
 * @since : 11/6/14,2014
 */
public class WritableList<E extends Writable> extends ArrayList<E> implements Writable{
	private Class<E> itemTypeClass;

	public WritableList(Class<E> typeClass){
		this.itemTypeClass = typeClass;
	}

	public WritableList(Class<E> typeClass,int initialCapacity){
		super(initialCapacity);
		this.itemTypeClass = typeClass;
	}


	/**
	 * <h3> Get item class by </h3>
	 * <pre>
	 * (Class<E>) ((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	 * </pre>
	 */
	@Deprecated
	public WritableList(){
		this.itemTypeClass = (Class<E>) ((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	private void check() throws IOException{
		if(this.itemTypeClass == null){
			throw new IOException("Class Type of WritableArrayList<E extends Writable> is null");
		}
	}

	public Class<E> getItemClass(){
		return itemTypeClass;
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws java.io.IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.check();
		out.writeInt(this.size());
		for(Writable item: this){
			item.write(out);
		}
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
		this.check();
		int size = in.readInt();
		for(int i=0;i<size;i++){
			try {
				E item = itemTypeClass.newInstance();
				item.readFields(in);
				this.add(item);
			} catch (InstantiationException e) {
				throw new IOException("Got exception to create instance for class: "+itemTypeClass+": "+e.getMessage(),e);
			} catch (IllegalAccessException e) {
				throw new IOException("Got exception to create instance for class: "+itemTypeClass+": "+e.getMessage(),e);
			}
		}
	}
}
