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
package org.apache.eagle.log.entity.meta;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetricDefinition implements Writable {
	private final static Logger LOG = LoggerFactory.getLogger(MetricDefinition.class);
	private long interval;
	private Class<?> singleTimestampEntityClass;
	public long getInterval() {
		return interval;
	}
	public void setInterval(long interval) {
		this.interval = interval;
	}
	public Class<?> getSingleTimestampEntityClass() {
		return singleTimestampEntityClass;
	}
	public void setSingleTimestampEntityClass(Class<?> singleTimestampEntityClass) {
		this.singleTimestampEntityClass = singleTimestampEntityClass;
	}

	private final static String EMPTY="";
	@Override
	public void write(DataOutput out) throws IOException {
		if(LOG.isDebugEnabled()) LOG.debug("Writing metric definition: interval = "+interval+" singleTimestampEntityClass = "+ this.singleTimestampEntityClass);
		out.writeLong(interval);
		if(this.singleTimestampEntityClass == null){
			out.writeUTF(EMPTY);
		}else {
			out.writeUTF(this.singleTimestampEntityClass.getName());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		interval = in.readLong();
		String singleTimestampEntityClassName = in.readUTF();
		if(!EMPTY.equals(singleTimestampEntityClassName)) {
			try {
				this.singleTimestampEntityClass = Class.forName(singleTimestampEntityClassName);
			} catch (ClassNotFoundException e) {
				if(LOG.isDebugEnabled()) LOG.warn("Class " + singleTimestampEntityClassName + " not found ");
			}
		}
	}
}
